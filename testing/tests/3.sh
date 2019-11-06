##
# Tests truncate implementation
##


if ! touch  $1/file_to_be_truncated; then
    exit 1
fi

if ! truncate -s 1000  $1/file_to_be_truncated; then
    exit 1
fi

if ! truncate -s 10000  $1/file_to_be_truncated; then
    exit 1
fi

if ! truncate -s 100000  $1/file_to_be_truncated; then
    exit 1
fi

if ! truncate -s 0  $1/file_to_be_truncated; then
    exit 1
fi
