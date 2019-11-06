###
# Given a size, this test fills a file with random data and crpytographically
# verifies the contents are unchanged.
# The size is in multiples of 4096 byte blocks.
# The process starts to slowdown between 5000 to 10000 and onwards
# Enough blocks are used to test doubly indirect indexing

# The test is also done for a subdirectory

###

function check()
{
    dd if=/dev/urandom of=data bs=4096 count=$1 > /dev/null 2>&1
    
    if ! cat data > $2/data; then
	exit 1
    fi

    checksum_a="$(md5sum data | awk '{ print $1 }')"
    checksum_b="$(md5sum $2/data | awk '{ print $1 }')"

    if [ "$checksum_a" != "$checksum_b" ]; then
	exit 1
    fi

    rm data
    rm $2/data;
}


check 10 $1
check 100 $1
check 300 $1
check 400 $1
check 1000 $1
check 2000 $1
       
if ! mkdir -p $1/file_check_sub_directory; then
    exit 1
fi

check 1000 $1/file_check_sub_directory


