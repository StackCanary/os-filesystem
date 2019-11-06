##
# Tests for the deletion of multiple files
##


if ! touch  $1/file_to_be_deleted1; then
    exit 1
fi

if ! touch  $1/file_to_be_deleted2; then
    exit 1
fi

if ! touch  $1/file_to_be_deleted3; then
    exit 1
fi


if ! rm  $1/file_to_be_deleted1; then
    exit 1
fi

if ! rm  $1/file_to_be_deleted2; then
    exit 1
fi

if ! rm  $1/file_to_be_deleted3; then
    exit 1
fi
