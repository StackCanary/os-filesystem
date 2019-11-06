##
# Tests chmod
##

if ! touch  $1/uid_gid_chmod_chown; then
    exit 1
fi


if ! chmod 0500 $1/uid_gid_chmod_chown; then
    exit 1
fi

if ! chmod 0640 $1/uid_gid_chmod_chown; then
    exit 1
fi

if ! chmod 0742 $1/uid_gid_chmod_chown; then
    exit 1
fi


   
