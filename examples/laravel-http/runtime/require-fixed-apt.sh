#!/bin/sh
# Read-only first-RUN boundary for the exact Bookworm/amd64 example parent.
# The Dockerfile invokes this through an absolute, credential-free environment.
set -eu

refuse() {
    printf '%s\n' 'Fixed APT prerequisite refused; no package operation was started.' >&2
    exit 1
}

[ "$#" -eq 0 ] || refuse
[ "$(/usr/bin/sed -n 's/^ID=//p' /etc/os-release)" = debian ] || refuse
[ "$(/usr/bin/sed -n 's/^VERSION_ID=//p' /etc/os-release)" = '"12"' ] || refuse
[ "$(/usr/bin/timeout 10 /usr/bin/dpkg --print-architecture)" = amd64 ] || refuse

installed_version() {
    record=$(/usr/bin/timeout 10 /usr/bin/dpkg-query --show \
        "--showformat=\${db:Status-Status}\t\${Version}\n" "$1") || refuse
    prefix=$(printf 'installed\t')
    case "$record" in "$prefix"*) version=${record#"$prefix"} ;; *) refuse ;; esac
    case "$version" in ''|[!0-9]*|*[!0-9A-Za-z.+:~-]*) refuse ;; esac
    [ "${#version}" -le 100 ] || refuse
    printf '%s' "$version"
}

apt_version=$(installed_version apt) || refuse
libapt_version=$(installed_version libapt-pkg6.0) || refuse
[ "$apt_version" = "$libapt_version" ] || refuse
/usr/bin/timeout 10 /usr/bin/dpkg --compare-versions "$apt_version" ge 2.6.2 || refuse
client=$(/usr/bin/timeout 10 /usr/bin/apt-get --version) || refuse
[ "$(printf '%s\n' "$client" | /usr/bin/sed -n '1p')" = "apt $apt_version (amd64)" ] || refuse
printf 'fixed-apt os=debian release=12 arch=amd64 apt=%s libapt-pkg6.0=%s\n' "$apt_version" "$libapt_version"
