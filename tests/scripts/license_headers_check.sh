#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

if [[ $# -eq 0 ]]
then
    echo "Usage: $0 check|fix"
    exit 1
fi

FILES=$(find . \( -iname "*.py" -or -iname "*.sh" \) -not -path "./venv/*")
for FILE in $FILES
do
    MISSING=$(grep --files-without-match "Licensed under the Elastic License 2.0" "$FILE")
    if [[ -n "$MISSING" ]]
    then
        if [[ "$1" = "fix" ]]
        then
            echo fix "$FILE"
            TMPFILE=$(mktemp /tmp/license.XXXXXXXXXX)
            if [[ "$FILE" == *".sh" && $(grep "#!/usr/bin/env bash" "$FILE") ]]
            then
                cat <<EOF > "$TMPFILE"
#!/usr/bin/env bash
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
EOF
            tail -n +2 "$FILE" >> "$TMPFILE"
            mv "$TMPFILE" "$FILE"
            chmod 755 "$FILE"
            else
                cat <<EOF > "$TMPFILE"
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

EOF
                cat "$FILE" >> "$TMPFILE"
                mv "$TMPFILE" "$FILE"
            fi
        else
            echo "File with missing copyright header:"
            echo "$MISSING"
            exit 1
        fi
    fi
done
