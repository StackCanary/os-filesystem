##
# This tests for file access/mod time
##

# getattr will be called automatically to verify the time is correct

if ! touch -t 200003101000 $1/file_for_time_checking; then
    exit 1
fi

if ! touch -t 199905111200 $1/file_for_time_checking; then
    exit 1
fi

if ! touch -t 201511111500 $1/file_for_time_checking; then
    exit 1
fi
