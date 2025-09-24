#!/bin/sh

export ENV_DATA_DIR="./sample.d"

geninput() {
	echo creating sample input files...

	mkdir -p ./sample.d

	printf '0123456789abcdef' | xxd -r -ps >./sample.d/timestamp.dat

	printf '\0\1' >./sample.d/severity.dat

	printf \
		'f123456789abcdef''0123456789abcdef' |
		xxd -r -ps >./sample.d/id.dat
}

test -f ./sample.d/timestamp.dat || geninput
test -f ./sample.d/severity.dat || geninput
test -f ./sample.d/id.dat || geninput

jq \
	-n \
	--argjson timestamp "$(jq -c -n '{
		name: "timestamp", nullable: false, endian: "Little", dtyp: "UInt32"
	}')" \
	--argjson severity "$(jq -c -n '{
		name: "severity", nullable: false, endian: "Unspecified", dtyp: "UInt8"
	}')" \
	--argjson id "$(jq -c -n '{
		name: "id", nullable: false, endian: "Big", dtyp: "Int64"
	}')" \
	'{
		fields: [
			$timestamp,
			$severity,
			$id
		]
	}' |
	./primitives2rbat
