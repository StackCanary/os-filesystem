##
# Tests for files/deletion/truncation etc inside sub directories
##

if ! mkdir -p $1/a/b; then
    exit 1
fi

if ! mkdir -p $1/c/d; then
    exit 1
fi

if ! touch $1/a/b/file_inside_sub_directory_1; then
    exit 1
fi

if ! touch $1/a/b/file_inside_sub_directory_2; then
    exit 1
fi

if ! touch -t 200003101000 $1/a/b/file_inside_sub_directory_1; then
    exit 1
fi

if ! rm $1/a/b/file_inside_sub_directory_1; then
    exit 1
fi
