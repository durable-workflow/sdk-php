<?php

declare(strict_types=1);

namespace DurableWorkflow\Codec;

use Apache\Avro\Datum\AvroIODatumReader;
use Apache\Avro\Datum\AvroIOBinaryDecoder;
use Apache\Avro\Datum\AvroIOSchemaMatchException;
use Apache\Avro\Schema\AvroNamedSchema;
use Apache\Avro\Schema\AvroSchema;
use Apache\Avro\Schema\AvroUnionSchema;

/** Apache PHP adapter for warning-free recursive named-union resolution. */
final class ValueDatumReader extends AvroIODatumReader
{
    /**
     * @param AvroSchema $writersSchema
     * @param AvroSchema $readersSchema
     * @param AvroIOBinaryDecoder $decoder
     */
    public function readData($writersSchema, $readersSchema, $decoder): mixed
    {
        if (
            $readersSchema instanceof AvroUnionSchema
            && ! $writersSchema instanceof AvroUnionSchema
        ) {
            foreach ($readersSchema->schemas() as $candidate) {
                if ($this->matches($writersSchema, $candidate)) {
                    return parent::readData($writersSchema, $candidate, $decoder);
                }
            }

            throw new AvroIOSchemaMatchException($writersSchema, $readersSchema);
        }

        return parent::readData($writersSchema, $readersSchema, $decoder);
    }

    private function matches(AvroSchema $writer, AvroSchema $reader): bool
    {
        if ($writer->type() !== $reader->type()) {
            return false;
        }
        if ($writer instanceof AvroNamedSchema && $reader instanceof AvroNamedSchema) {
            $aliases = $reader->getAliases();

            return $writer->fullname() === $reader->fullname()
                || (is_array($aliases) && in_array($writer->fullname(), $aliases, true));
        }

        return true;
    }
}
