#!/usr/bin/env bash
set -euo pipefail

# Source checks use the browser already provided by the Ubuntu x64 runner.
# Missing or changed stock prerequisites refuse without an installer fallback.
chrome_binary=/opt/google/chrome/chrome
printf 'Runner browser architecture: %s\n' "$(/usr/bin/uname -m)"
if [[ -e "$chrome_binary" || -L "$chrome_binary" ]]; then
  /usr/bin/stat -c 'Browser file: %n type=%F owner=%u:%g mode=%a' "$chrome_binary"
  printf 'Canonical browser path: %s\n' "$(/usr/bin/readlink -f "$chrome_binary")"
else
  echo 'The fixed Chrome executable is absent.'
fi
if [[ $(/usr/bin/uname -m) != x86_64 || ! -f "$chrome_binary" || ! -x "$chrome_binary" || -L "$chrome_binary" \
  || $(/usr/bin/readlink -f "$chrome_binary") != "$chrome_binary" \
  || $(/usr/bin/stat -c '%u:%g:%a' "$chrome_binary") != 0:0:755 ]]; then
  echo 'The runner must provide the root-owned Google Chrome executable.' >&2
  exit 1
fi

package=$(/usr/bin/dpkg-query -W -f='${binary:Package}\t${Architecture}\t${Status}\t${Version}' google-chrome-stable)
package_pattern=$'^google-chrome-stable\tamd64\tinstall ok installed\t[0-9][A-Za-z0-9.+:~_-]*$'
if [[ ! "$package" =~ $package_pattern \
  || $(/usr/bin/dpkg-query -S "$chrome_binary") != "google-chrome-stable: $chrome_binary" ]]; then
  echo 'The browser must belong to the installed stock Chrome package.' >&2
  exit 1
fi

printf 'Runner browser package: %s\n' "$package"
/usr/bin/sha256sum "$chrome_binary"
"$chrome_binary" --version
printf 'PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH=%s\n' "$chrome_binary" >> "${GITHUB_ENV:?GitHub Actions environment file is required}"
