##
# This tests for the recursive deletion of multiple directories 
##


if ! mkdir -p $1/a/b/c/d/e/f; then
    exit 1
fi

if ! mkdir -p $1/g/h/i/j/k/l; then
    exit 1
fi

if ! mkdir -p $1/m/n/o/p/q/r; then
    exit 1
fi

if ! rm -r $1/a/; then
    exit 1
fi

if ! rm -r $1/g/; then
    exit 1
fi

if ! rm -r $1/m/; then
    exit 1
fi

