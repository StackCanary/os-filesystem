/*
  MyFS. One directory, one file, 1000 bytes of storage. What more do you need?
  
  This Fuse file system is based largely on the HelloWorld example by Miklos Szeredi <miklos@szeredi.hu> (http://fuse.sourceforge.net/helloworld.html). Additional inspiration was taken from Joseph J. Pfeiffer's "Writing a FUSE Filesystem: a Tutorial" (http://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/).
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <fcntl.h>

#include <assert.h>

#include "myfs.h"

#define next_multiple_of(x, m) (((x) + ((m)-1)) & ~((m)-1))
#define size_to_block(x) ((int)((x) / BLOCK_SIZE))

// This is the pointer to the database we will use to store all our files
unqlite *pDb;
uuid_t zero_uuid;
static indirect_block_t empty_indirect_block = {0};

myfs_hashtable_t *hashtable = NULL;
myfs_node_t *root = NULL;

myfcb cached_root_fcb = {0};



void print_fcb(myfcb *inode)
{
  printf("\n");
  printf("uid %x\n", inode->uid);
  printf("gid %x\n", inode->gid);
  printf("mode %x\n", inode->mode);
  printf("mtime %lu\n", inode->mtime);
  printf("size %lu\n", inode->size);
  printf("\n");
}




int db_put_block(uuid_t key, void *data, size_t size)
{

  int rc = 0;

  if (uuid_is_null(key))
    return unqlite_kv_store(pDb, key, KEY_SIZE, data, size);
  

  myfs_node_t * cached_data = myfs_hashtable_get(hashtable, key); 
  
  if (cached_data) {

    // Write data to cached data
    memcpy(cached_data->data, data, size);

    // Move found cached data to top of queue
    myfs_queue_rem(cached_data);
    myfs_queue_top(root, cached_data);
    
  } else {

    myfs_node_t * to_be_cached = myfs_mk_node(key, data, size);

    bool evict = (hashtable->s >= CACHE_EVICT_SIZE);

    if (evict) {

      myfs_node_t *to_be_evicted = root->prev;

      unqlite_kv_store(pDb, to_be_evicted->key, KEY_SIZE, to_be_evicted->data, to_be_evicted->size);

      cache_evict(hashtable, root, to_be_evicted);

    }

    myfs_queue_top(root, to_be_cached);
    myfs_hashtable_put(hashtable, to_be_cached);
    
  }

  return rc;
}

int db_get_block(uuid_t key, void *data, unqlite_int64 size)
{

  int rc = 0;

  if (uuid_is_null(key))
    return unqlite_kv_fetch(pDb, key, KEY_SIZE, data, &size);

  myfs_node_t * cached_data = myfs_hashtable_get(hashtable, key); 
  
  if (cached_data) {

    memcpy(data, cached_data->data, size);

    myfs_queue_rem(cached_data);
    myfs_queue_top(root, cached_data);
    
  } else {

    unqlite_kv_fetch(pDb, key, KEY_SIZE, data, &size);

    myfs_node_t * to_be_cached = myfs_mk_node(key, data, size);

    bool evict = (hashtable->s >= CACHE_EVICT_SIZE);

    if (evict) {

      myfs_node_t *to_be_evicted = root->prev;
      
      unqlite_kv_store(pDb, to_be_evicted->key, KEY_SIZE, to_be_evicted->data, to_be_evicted->size);

      cache_evict(hashtable, root, to_be_evicted);

    } 

    myfs_queue_top(root, to_be_cached);
    myfs_hashtable_put(hashtable, to_be_cached);
    
  }

  return rc;
}

int db_put(uuid_t key, void *data, size_t size)
{
  return unqlite_kv_store(pDb, key, KEY_SIZE, data, size);
}

int db_get(uuid_t key, void *data, unqlite_int64 size)
{
  return unqlite_kv_fetch(pDb, key, KEY_SIZE, data, &size);
}


int db_rem(uuid_t key)
{
  unqlite_kv_delete(pDb, key, KEY_SIZE);
}

void print_uuid(uuid_t uuid)
{

  char str[64];
  uuid_unparse_lower(uuid, str);
  printf("uuid %s\n", str);
}

bool split_path(char **p, char **s)
{
  bool f = false;
  
  (*p)++;
  (*s) = (*p);
  while(**p != '/' && **p != '\0')
    (*p)++;

  if (**p == '\0')
    f = true;

  **p = '\0';

  return f;
}

void ascend_path(char *p)
{
  char * s = p;
  
  p = (strlen(p) - 1) + p;
 
  while(*p != '/')
    p--;

  *p = '\0';

  if (s == p) {
    *p++ = '/';
    *p++ = '\0';
  }
}

/* Takes a directory inode and returns the given filename's dirent (filename -> uuid)
 */
bool search_file(const char* name, myfcb *directory_inode, dirent_t *dirent)
{
  bool retval = false;
  
  if (!directory_inode->file_data_id)
    return retval;

  if (!directory_inode->size)
    return retval;

  dirent_t * dirents = calloc(directory_inode->size, sizeof(dirent_t));

  db_get(directory_inode->file_data_id, dirents, directory_inode->size * sizeof(dirent_t));
  
  for (int i = 0; i < directory_inode->size; i++) {
    if (strcmp(dirents[i].name, name) == 0) {
      *dirent = dirents[i];
      retval = true;
      break;
    }
  }

  free(dirents);

  return retval;
}




int app(uuid_t uuid, void *item, size_t size)
{
  return unqlite_kv_append(pDb, uuid, KEY_SIZE, item, size);
}

int get_root_inode()
{
  return db_get(ROOT_OBJECT_KEY, &cached_root_fcb, sizeof(myfcb));
}

int set_root_inode()
{
  return db_put(ROOT_OBJECT_KEY, &cached_root_fcb, sizeof(myfcb));
}

/*
  Appends a directory entry into a directory
 */
void add_dirent(uuid_t parent_uuid, myfcb* directory, dirent_t dirent)
{
  directory->size++;
  app(directory->file_data_id, &dirent, sizeof(dirent_t));
}

dirent_t mk_dirent(char *fname, uuid_t uuid)
{
  dirent_t d = {0};
  strcpy(d.name, fname);
  uuid_copy(d.uuid, uuid);
  return d;
}

bool descend(char *name, myfcb *current_directory, uuid_t parent_uuid)
{
  dirent_t dirent = {0};

  if (search_file(name, current_directory, &dirent)) {

    memset(current_directory, 0, sizeof(myfcb));

    int rc = db_get(dirent.uuid, current_directory, sizeof(myfcb));

    assert(rc == UNQLITE_OK);

    uuid_copy(parent_uuid, dirent.uuid);
    
    return true;
  }

  return false;
}


bool traverse(const char *path, uuid_t fcb_uuid, myfcb *current_directory)
{

  memset(current_directory, 0, sizeof(myfcb));
  
  *current_directory = cached_root_fcb;

  uuid_copy(fcb_uuid, zero_uuid);
  
  char copy[strlen(path) + 1]; strcpy(copy, path);
  
  char *p = copy;
  char *s = NULL;

  bool finished = strcmp(path, "/") == 0;

  while(!finished) {

    finished = split_path(&p, &s);

    if (!descend(s, current_directory, fcb_uuid))
      return false;

  }

  return true;
  
}

/*
  Creates an inode representing a file/directory and inserts it into a parents directory 
 */
myfcb create_inode(uuid_t parent_uuid, myfcb *parent_directory, char *filename, bool is_directory, mode_t mode)
{

  myfcb fcb;
  memset(&fcb, 0, sizeof(myfcb));

  uuid_generate_random(fcb.file_data_id);

  fcb.mode = mode;
  fcb.mtime = time(0);
  fcb.ctime = time(0);
  fcb.atime = time(0);
  fcb.uid = getuid();
  fcb.gid = getgid();
  fcb.nlink++;

  if (is_directory)
    fcb.mode |= S_IFDIR;


  uuid_t uuid_to_fcb;
  uuid_generate_random(uuid_to_fcb);
		
  int rc = unqlite_kv_store(pDb, uuid_to_fcb, KEY_SIZE, &fcb, sizeof(myfcb));

  if( rc != UNQLITE_OK )
    error_handler(rc);

  dirent_t dirent = {0};
  strcpy(dirent.name, filename);
  uuid_copy(dirent.uuid, uuid_to_fcb);

  rc = app(parent_directory->file_data_id, &dirent, sizeof(dirent_t));

  assert(rc == UNQLITE_OK);

  // Don't forget to update the stored fcb (later)
  parent_directory->size++;

  db_put(parent_uuid, parent_directory, sizeof(myfcb));
  
  if (uuid_is_null(parent_uuid))
    cached_root_fcb = *parent_directory;


  return fcb;
}


void fill_stbuf(struct stat *stbuf, myfcb *inode, ino_t number)
{
  stbuf->st_ino   = number;
  stbuf->st_mode  = inode->mode;
  stbuf->st_uid   = inode->uid;
  stbuf->st_gid   = inode->gid;
  stbuf->st_size  = inode->size;
  stbuf->st_mtime = inode->mtime;
  stbuf->st_ctime = inode->ctime;
  stbuf->st_atime = inode->atime;
}

// The functions which follow are handler functions for various things a filesystem needs to do:
// reading, getting attributes, truncating, etc. They will be called by FUSE whenever it needs
// your filesystem to do something, so this is where functionality goes.

// Get file and directory attributes (meta-data).
// Read 'man 2 stat' and 'man 2 chmod'.
static int myfs_getattr(const char *path, struct stat *stbuf)
{
  write_log("myfs_getattr(path=\"%s\", statbuf=0x%08x)\n", path, stbuf);
  
  memset(stbuf, 0, sizeof(struct stat));

  myfcb current_directory = cached_root_fcb;


  uuid_t parent_uuid;
  uuid_copy(parent_uuid, zero_uuid);
  
  char copy[strlen(path) + 1];
  strcpy(copy, path);
  
  char *p = copy;
  char *s = NULL;

  bool finished = strcmp(path, "/") == 0;

  while(!finished) {

    finished = split_path(&p, &s);

    if (!descend(s, &current_directory, parent_uuid))
      return -ENOENT;


  }

  fill_stbuf(stbuf, &current_directory, 10);
  
  return 0;
}

// Read a directory.
// Read 'man 2 readdir'.
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
  write_log("write_readdir(path=\"%s\", buf=0x%08x, filler=0x%08x, offset=%lld, fi=0x%08x)\n", path, buf, filler, offset, fi);

  filler(buf, ".", NULL, 0);
  filler(buf, "..", NULL, 0);

  myfcb current_directory = cached_root_fcb;

  uuid_t parent_uuid;
  uuid_copy(parent_uuid, zero_uuid);
  
  char copy[strlen(path) + 1];
  strcpy(copy, path);
  
  char *p = copy;
  char *s = NULL;

  bool finished = strcmp(path, "/") == 0;

  while(!finished) {

    finished = split_path(&p, &s);

    if (!descend(s, &current_directory, parent_uuid))
      return -ENOENT;


  }
  
  if (current_directory.size) {
    
    dirent_t * entries = calloc(current_directory.size, sizeof(dirent_t));

    db_get(current_directory.file_data_id, entries, current_directory.size * sizeof(dirent_t));

    for (int i = 0; i < current_directory.size; i++)
      filler(buf, entries[i].name, NULL, 0);

    free(entries);
  }
  
  return 0;
}


static void get_singlely_block_uuid(myfcb *fcb, int index, uuid_t uuid_to_block)
{
  index = index - 13;

  indirect_block_t indirect_block = {0};

  db_get_block(fcb->singley_indirect_blocks, &indirect_block, sizeof(indirect_block_t));
  
  uuid_copy(uuid_to_block, indirect_block.uuid[index]);
}

static void get_doubley_block_uuid(myfcb *fcb, int index, uuid_t uuid_to_block)
{

  index = index - 13 - 256;

  int fst_index = index / 256;
  int snd_index = index % 256;

  indirect_block_t indirect_block_f = {0};
  indirect_block_t indirect_block_s = {0};

  db_get_block(fcb->doubley_indirect_blocks, &indirect_block_f, sizeof(indirect_block_t));
  db_get_block(indirect_block_f.uuid[fst_index], &indirect_block_s, sizeof(indirect_block_t));

  uuid_copy(uuid_to_block, indirect_block_s.uuid[snd_index]);
}

static void get_block_uuid(myfcb *fcb, int index, uuid_t uuid_to_block)
{
      
  if (index < 13) {

    uuid_copy(uuid_to_block, fcb->direct_blocks[index]);
    
  } else if (index < 256 + 13) {

    get_singlely_block_uuid(fcb, index, uuid_to_block);

    
  } else if (index < 256 + 13 + 256 * 256) {

    get_doubley_block_uuid(fcb, index, uuid_to_block);
    
  }
    
  
}

static void uninitialize_block(uuid_t uuid_to_indirect_block)
{
  db_rem(uuid_to_indirect_block);
  uuid_copy(uuid_to_indirect_block, zero_uuid);
}

static void rem_direct_block(uuid_t uuid_of_fcb, myfcb *fcb, int index)
{
  uninitialize_block(fcb->direct_blocks[index]);
}

static void rem_singley_indirect_block(uuid_t uuid_of_fcb, myfcb *fcb, int index)
{
  index = index - 13;

  indirect_block_t indirect_block_f = {0};

  db_get_block(fcb->singley_indirect_blocks, &indirect_block_f, sizeof(indirect_block_t));

  uninitialize_block(indirect_block_f.uuid[index]);

  db_put_block(fcb->singley_indirect_blocks, &indirect_block_f, sizeof(indirect_block_t));

  if (index == 0)
    uninitialize_block(fcb->singley_indirect_blocks);

}

static void rem_doubley_indirect_block(uuid_t uuid_of_fcb, myfcb *fcb, int index)
{
  index = index - 13 - 256;

  int fst_index = index / 256;
  int snd_index = index % 256;

  indirect_block_t indirect_block_f = {0};
  indirect_block_t indirect_block_s = {0};

  // Uninitialize doubley indirect block

  db_get_block(fcb->doubley_indirect_blocks, &indirect_block_f, sizeof(indirect_block_t));
  db_get_block(indirect_block_f.uuid[fst_index], &indirect_block_s, sizeof(indirect_block_t));

  uninitialize_block(indirect_block_s.uuid[snd_index]);

  if (snd_index == 0)
    uninitialize_block(indirect_block_f.uuid[fst_index]);

  if (index == 0)
    uninitialize_block(fcb->doubley_indirect_blocks);
  
}

static void rem_block(uuid_t uuid_of_fcb, myfcb *fcb, int index)
{
  if (index < 13) {

    rem_direct_block(uuid_of_fcb, fcb, index);

  } else if (index < 256 + 13) {

    rem_singley_indirect_block(uuid_of_fcb, fcb, index);
    
  }  else if (index < 256 + 13 + 256 * 256) {
    
    rem_doubley_indirect_block(uuid_of_fcb, fcb, index);
    
  }

  db_put(uuid_of_fcb, fcb, sizeof(fcb));
}


static void initialize_block(uuid_t uuid_to_indirect_block)
{
  uuid_generate_random(uuid_to_indirect_block);
  db_put_block(uuid_to_indirect_block, &empty_indirect_block, sizeof(indirect_block_t));
}

static void add_direct_block(myfcb *fcb, int index)
{
  initialize_block(fcb->direct_blocks[index]);
}

static void add_singley_indirect_block(uuid_t uuid_of_fcb, myfcb *fcb, int index)
{
  indirect_block_t indirect_block_f = {0};

  index = index - 13;

  // Initialize singley indirect block
  if (index == 0)
    initialize_block(fcb->singley_indirect_blocks);

  db_get_block(fcb->singley_indirect_blocks, &indirect_block_f, sizeof(indirect_block_t));

  initialize_block(indirect_block_f.uuid[index]);


  db_put_block(fcb->singley_indirect_blocks, &indirect_block_f, sizeof(indirect_block_t));
}

static void add_doubley_indirect_block(uuid_t uuid_of_fcb, myfcb *fcb, int index)
{
  indirect_block_t indirect_block_f = {0};
  indirect_block_t indirect_block_s = {0};

  index = index - 13 - 256;

  int fst_index = index / 256;
  int snd_index = index % 256;

  // Initialize doubley indirect block
  if (index == 0)
    initialize_block(fcb->doubley_indirect_blocks);

  db_get_block(fcb->doubley_indirect_blocks, &indirect_block_f, sizeof(indirect_block_t));

  if (snd_index == 0)
    initialize_block(indirect_block_f.uuid[fst_index]);

  db_get_block(indirect_block_f.uuid[fst_index], &indirect_block_s, sizeof(indirect_block_t));

  initialize_block(indirect_block_s.uuid[snd_index]);

  db_put_block(fcb->doubley_indirect_blocks, &indirect_block_f, sizeof(indirect_block_t));
  db_put_block(indirect_block_f.uuid[fst_index], &indirect_block_s, sizeof(indirect_block_t));
  
}

static void add_block(uuid_t uuid_of_fcb, myfcb *fcb, int index)
{

  if (index < 13) {

    add_direct_block(fcb, index);

  } else if (index < 256 + 13) {

    add_singley_indirect_block(uuid_of_fcb, fcb, index);
    
  } else if (index < 256 + 13 + 256 * 256) {

    add_doubley_indirect_block(uuid_of_fcb, fcb, index);
    
  }

}

static void set_block(myfcb *fcb, int index, block_t *block)
{

  uuid_t uuid_to_block;
  get_block_uuid(fcb, index, uuid_to_block);
    
  db_put_block(uuid_to_block, block, sizeof(block_t));
    
}

static void get_block(myfcb *fcb, int index, block_t *block)
{

  uuid_t uuid_to_block;
  get_block_uuid(fcb, index, uuid_to_block);
    
  db_get_block(uuid_to_block, block, sizeof(block_t));
  
}


static int _internal_resize_(uuid_t uuid_of_fcb, myfcb *fcb, size_t newsize)
{
  size_t blocks_supplied = size_to_block(fcb->size) + (fcb->size % BLOCK_SIZE != 0);
  size_t blocks_required = size_to_block(newsize)   + 1;

  if (blocks_required > blocks_supplied) {

    for (int i = blocks_supplied; i < blocks_required; i++)
      add_block(uuid_of_fcb, fcb, i);
    
  }

  if (blocks_required < blocks_supplied) {

    for (int i = blocks_required - 1; i >= blocks_required; i--)
      rem_block(uuid_of_fcb, fcb, i);

  }

  fcb->size = newsize;
  
  db_put(uuid_of_fcb, fcb, sizeof(myfcb));
    
}

static int _internal_put_(myfcb *fcb, const char *buf, size_t bytes, off_t start)
{

  block_t block;

  int i = size_to_block(start); // block to start with
  
  while(bytes) {

    size_t s = start % (BLOCK_SIZE);
    size_t r = (BLOCK_SIZE - s);
    size_t l = bytes < r ? bytes : r;

    start = 0;

    memset(&block, 0, sizeof(block_t));
    get_block(fcb, i, &block);

    char *dst = ((char *) (&block)) + s;
    
    memcpy(dst, buf, l);

    set_block(fcb, i, &block);

    bytes -= l;
    buf   += l;
    
    i++;
  }
    
  
  return 0;
}

static int _internal_get_(myfcb *fcb, char *buf, size_t bytes, off_t start)
{

  block_t block;

  int i = size_to_block(start); // block to start with
  
  while(bytes) {

    size_t s = start % (BLOCK_SIZE);
    size_t r = (BLOCK_SIZE - s);
    size_t l = bytes < r ? bytes : r;

    start = 0;

    memset(&block, 0, sizeof(block_t));
    get_block(fcb, i, &block);

    char *dst = ((char *) (&block)) + s;
    
    memcpy(buf, dst, l);

    bytes -= l;
    buf   += l;
    
    i++;
  }
  
  return 0;
}


// Read a file.
// Read 'man 2 read'.
static int myfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
  write_log("myfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);

  // TODO
  myfcb fcb = {0}; uuid_t uuid; uuid_copy(uuid, zero_uuid);

  if( traverse(path, uuid, &fcb) ) {

    size_t corrected_size = fcb.size < size ? fcb.size : size;

    _internal_get_(&fcb, buf, corrected_size, offset);
    
    return corrected_size;
  } 
  
  return -ENOENT;
}

// This file system only supports one file. Create should fail if a file has been created. Path must be '/<something>'.
// Read 'man 2 creat'.
static int myfs_create(const char *path, mode_t mode, struct fuse_file_info *fi){   
  write_log("myfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n", path, mode, fi);

  myfcb current_directory = cached_root_fcb;

  uuid_t parent_uuid;
  uuid_copy(parent_uuid, zero_uuid);
  
  char copy[strlen(path) + 1]; strcpy(copy, path);
  
  char *p = copy;
  char *s = NULL;

  while(!split_path(&p, &s)) {

    if (!descend(s, &current_directory, parent_uuid))
      return -ENOENT;

  }

  create_inode(parent_uuid, &current_directory, s, false, mode);
  
  return 0;
}

// Set update the times (actime, modtime) for a file. This FS only supports modtime.
// Read 'man 2 utime'.
static int myfs_utime(const char *path, struct utimbuf *ubuf){
  write_log("myfs_utime(path=\"%s\", ubuf=0x%08x)\n", path, ubuf);

  myfcb fcb = {0}; uuid_t uuid; uuid_copy(uuid, zero_uuid);

  if( traverse(path, uuid, &fcb) ) {
    
    fcb.atime = ubuf->actime;
    fcb.mtime = ubuf->modtime;

    db_put(uuid, &fcb, sizeof(myfcb));
    
    return 0;
  }
  
  return -ENOENT;
}

// Write to a file.
// Read 'man 2 write'
static int myfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){   
  write_log("myfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n", path, buf, size, offset, fi);

  myfcb fcb = {0}; uuid_t uuid; uuid_copy(uuid, zero_uuid);

  if( traverse(path, uuid, &fcb) ) {
    
    if(fcb.size < offset + size) {
      //      internal_truncate(uuid, &fcb, offset + size);

      _internal_resize_(uuid, &fcb, offset + size);
      
    }

    unsigned char * contents = calloc(sizeof(unsigned char), fcb.size);

    //    memcpy(contents + offset, buf, size);

    _internal_put_(&fcb, buf, size, offset);

      //    db_put(fcb.file_data_id, contents, fcb.size);

    free(contents);
    
    return size;
  } 
  
  return -ENOENT;
}

// Set the size of a file.
// Read 'man 2 truncate'.
int myfs_truncate(const char *path, off_t newsize){    
  write_log("myfs_truncate(path=\"%s\", newsize=%lld)\n", path, newsize);

  myfcb fcb = {0}; uuid_t uuid; uuid_copy(uuid, zero_uuid);

  if( traverse(path, uuid, &fcb) ) {


    _internal_resize_(uuid, &fcb, newsize);

    //    internal_truncate(uuid, &fcb, newsize);
    
    return 0;
  } 
  
  return -ENOENT;
}

// Set permissions.
// Read 'man 2 chmod'.
int myfs_chmod(const char *path, mode_t mode){
  write_log("myfs_chmod(fpath=\"%s\", mode=0%03o)\n", path, mode);

  myfcb fcb = {0}; uuid_t uuid; uuid_copy(uuid, zero_uuid);

  if( traverse(path, uuid, &fcb) ) {
    
    fcb.mode = mode;

    db_put(uuid, &fcb, sizeof(myfcb));
    
    return 0;
  }
  
    
  return -ENOENT;
}

// Set ownership.
// Read 'man 2 chown'.
int myfs_chown(const char *path, uid_t uid, gid_t gid){   
  write_log("myfs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);

  myfcb fcb = {0}; uuid_t uuid; uuid_copy(uuid, zero_uuid);

  if( traverse(path, uuid, &fcb) ) {
    
    fcb.uid = uid;
    fcb.gid = gid;

    db_put(uuid, &fcb, sizeof(myfcb));
    
    return 0;
  }
   
  return -ENOENT;
}

// Create a directory.
// Read 'man 2 mkdir'.
static int myfs_mkdir(const char *path, mode_t mode)
{
  myfcb current_directory = cached_root_fcb;

  uuid_t parent_uuid;
  uuid_copy(parent_uuid, zero_uuid);
  
  char copy[strlen(path) + 1]; strcpy(copy, path);
  
  char *p = copy;
  char *s = NULL;

  while(!split_path(&p, &s)) {

    if (!descend(s, &current_directory, parent_uuid))
      return -ENOENT;

  }

  create_inode(parent_uuid, &current_directory, s, true, mode);

  return 0;
}


bool rm_dirent(uuid_t parent_uuid, myfcb *directory, char *file)
{
  bool found = false;

  size_t size = directory->size;
  
  if (size > 0) {

    dirent_t * dirents = calloc(size, sizeof(dirent_t));

    db_get(directory->file_data_id, dirents, size * sizeof(dirent_t));
  
    for (int i = 0; i < size; i++) {
      if (strcmp(dirents[i].name, file) == 0) {

	found = true;

	myfcb fcb = {0};

	db_get(dirents[i].uuid, &fcb, sizeof(fcb));

	if (S_ISDIR(fcb.mode))
	  db_rem(fcb.file_data_id);
	else
	  _internal_resize_(dirents[i].uuid, &fcb, 0);

	db_rem(dirents[i].uuid);

	for (int j = i; j < size - 1; j++)
	  dirents[j] = dirents[j + 1];
	
	break;
      }
    }

    directory->size--;

    if (found)
      if (size - 1 > 0)
	db_put(directory->file_data_id, dirents, (size - 1) * sizeof(dirent_t));
      else
	db_rem(directory->file_data_id);

    if (uuid_is_null(parent_uuid))
      cached_root_fcb = *directory;
  
    free(dirents);
    
  }

  return found;
}

// Delete a file.
// Read 'man 2 unlink'.
int myfs_unlink(const char *path){
  write_log("myfs_unlink: %s\n",path);

  char parent[strlen(path) + 1]; strcpy(parent, path); ascend_path(parent);

  char copy_of_path[strlen(path) + 1]; strcpy(copy_of_path, path);

  char *p = copy_of_path; char *s = NULL;

  while(!split_path(&p, &s))
    ;

  myfcb fcb = {0}; uuid_t uuid; uuid_copy(uuid, zero_uuid);

  traverse(parent, uuid, &fcb);

  rm_dirent(uuid, &fcb, s);
	
  return 0;
}



// Delete a directory.
// Read 'man 2 rmdir'.
int myfs_rmdir(const char *path){
  write_log("myfs_rmdir: %s\n",path);

  char parent[strlen(path) + 1]; strcpy(parent, path); ascend_path(parent);

  char copy_of_path[strlen(path) + 1]; strcpy(copy_of_path, path);

  char *p = copy_of_path; char *s = NULL;

  while(!split_path(&p, &s))
    ;

  myfcb fcb = {0}; uuid_t uuid; uuid_copy(uuid, zero_uuid);

  traverse(parent, uuid, &fcb);

  rm_dirent(uuid, &fcb, s);
  
  return 0;
}

// OPTIONAL - included as an example
// Flush any cached data.
int myfs_flush(const char *path, struct fuse_file_info *fi){
  int retstat = 0;
  
  write_log("myfs_flush(path=\"%s\", fi=0x%08x)\n", path, fi);

  flush_cache(root);
	
  return retstat;
}

// OPTIONAL - included as an example
// Release the file. There will be one call to release for each call to open.
int myfs_release(const char *path, struct fuse_file_info *fi){
  write_log("myfs_release(path=\"%s\", fi=0x%08x)\n", path, fi);
    
  return -ENOENT;
}

// OPTIONAL - included as an example
// Open a file. Open should check if the operation is permitted for the given flags (fi->flags).
// Read 'man 2 open'.
static int myfs_open(const char *path, struct fuse_file_info *fi){
  write_log("myfs_open(path\"%s\", fi=0x%08x)\n", path, fi);

  // TODO
  
  return 0;
}

// Initialise the in-memory data structures from the store. If the root object (from the store) is empty then create a root fcb (directory)
// and write it to the store. Note that this code is executed outide of fuse. If there is a failure then we have failed toi initlaise the 
// file system so exit with an error code.
void init_fs(){
  
  int rc;
  printf("init_fs\n");
  //Initialise the store.
    
  uuid_clear(zero_uuid);
	
  // Open the database.
  rc = unqlite_open(&pDb,DATABASE_NAME,UNQLITE_OPEN_CREATE);
  if( rc != UNQLITE_OK ) error_handler(rc);

  unqlite_int64 nBytes = sizeof(myfcb);  // Data length


  hashtable = myfs_mk_hashtable();
  root = myfs_mk_root();

  // Try to fetch the root element
  // The last parameter is a pointer to a variable which will hold the number of bytes actually read


  rc = db_get(ROOT_OBJECT_KEY, &cached_root_fcb, nBytes);


  // if it doesn't exist, we need to create one and put it into the database. This will be the root
  // directory of our filesystem i.e. "/"
  if(rc==UNQLITE_NOTFOUND) {      

    printf("init_store: root object was not found\n");

    // clear everything in the_root_fcb
    memset(&cached_root_fcb, 0, sizeof(myfcb));
				
    // Sensible initialisation for the root FCB
    //See 'man 2 stat' and 'man 2 chmod'.
    uuid_generate_random(cached_root_fcb.file_data_id);
    cached_root_fcb.mode |= S_IFDIR|S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH; 
    cached_root_fcb.mtime = time(0);
    cached_root_fcb.ctime = time(0);
    cached_root_fcb.atime = time(0);
    cached_root_fcb.uid = getuid();
    cached_root_fcb.gid = getgid();
    cached_root_fcb.nlink = 1;
		
    // Write the root FCB
    printf("init_fs: writing root fcb\n");
    rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, KEY_SIZE, &cached_root_fcb, sizeof(myfcb));

    if( rc != UNQLITE_OK ) error_handler(rc);
  } 
  else
    {
      if(rc==UNQLITE_OK) { 
	printf("init_store: root object was found\n"); 
      }
      if(nBytes!=sizeof(myfcb)) { 
	printf("Data object has unexpected size. Doing nothing.\n");
	exit(-1);
      }
 
    }
       
}

void shutdown_fs(){

  flush_cache(root);

  unqlite_close(pDb);
}

 
static struct fuse_operations myfs_oper = {
  .getattr	= myfs_getattr,
  .readdir	= myfs_readdir,
  .open		= myfs_open,
  .read		= myfs_read,
  .create		= myfs_create,
  .utime 		= myfs_utime,
  .write		= myfs_write,
  .truncate	= myfs_truncate,
  .flush		= myfs_flush,
  .release	= myfs_release,
  .mkdir = myfs_mkdir,
  .rmdir = myfs_rmdir,
  .unlink = myfs_unlink,
  .chmod = myfs_chmod,
  .chown = myfs_chown,
};

int main(int argc, char *argv[]){	
  int fuserc;
  struct myfs_state *myfs_internal_state;

  //Setup the log file and store the FILE* in the private data object for the file system.	
  myfs_internal_state = malloc(sizeof(struct myfs_state));
  myfs_internal_state->logfile = init_log_file();
	
  //Initialise the file system. This is being done outside of fuse for ease of debugging.
  init_fs();
	       
  // Now pass our function pointers over to FUSE, so they can be called whenever someone
  // tries to interact with our filesystem. The internal state contains a file handle
  // for the logging mechanism
  fuserc = fuse_main(argc, argv, &myfs_oper, myfs_internal_state);
	
  //Shutdown the file system.
  shutdown_fs();
	
  return fuserc;
}

