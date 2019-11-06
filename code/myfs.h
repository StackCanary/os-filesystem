//#include "fs.h"
#include <uuid/uuid.h>
#include <unqlite.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <fuse.h>

#define KEY_SIZE 16

// This is a starting File Control Block for the 
// simplistic implementation provided.
//
// It combines the information for the root directory "/"
// and one single file inside this directory. This is why there
// is a one file limit for this filesystem
//
// Obviously, you will need to be change this into a
// more sensible FCB to implement a proper filesystem

#define DIRECT_BLOCKS 13
#define BLOCKS 15

#define SINGLE_INDIRECT_BLOCKS 256


typedef struct _myfcb
{
  // see 'man 2 stat' and 'man 2 chmod'
  //meta-data for the 'file'
  uuid_t file_data_id;
  
  uid_t  uid;     /* user */
  gid_t  gid;     /* group */
  mode_t mode;    /* protection */
  time_t atime;
  time_t mtime;   /* time of last modification */
  time_t ctime;   /* time of last change to meta-data (status) */
  nlink_t nlink;
  off_t size;     /* size */


  struct
  {
    uuid_t direct_blocks[DIRECT_BLOCKS];
    uuid_t singley_indirect_blocks;
    uuid_t doubley_indirect_blocks;
  };

    
} myfcb;


#define MAX_FILE_NAME 239

/* 
   Size per entry is (name:240 + uuid:16) = 256 bytes, 16 entries per block
 */
typedef struct _dirent
{
  union
  {
    struct
    {
      char name[MAX_FILE_NAME + 1];
      uuid_t uuid;
    };
      
  };


} dirent_t;

#define BLOCK_SIZE (4096)

/*
  Size per entry is 16 bytes, 256 entries per block
 */
typedef struct _indirection_block_
{
  uuid_t uuid[BLOCK_SIZE / 16];
} indirect_block_t;

/*
  A block (4096 bytes)
 */
typedef struct _block_
{
  union
  {
    struct
    {
      unsigned char block[BLOCK_SIZE];
    };

    struct
    {
      dirent_t dirents[BLOCK_SIZE / sizeof(dirent_t)];
    };
      
  };
  
} block_t;



// Some other useful definitions we might need

extern unqlite_int64 root_object_size_value;

// We need to use a well-known value as a key for the root object.
/* #define ROOT_OBJECT_KEY "root" */
/* #define ROOT_OBJECT_KEY_SIZE 4 */


#define ROOT_OBJECT_KEY zero_uuid
#define ROOT_OBJECT_KEY_SIZE 16

// This is the size of a regular key used to fetch things from the 
// database. We use uuids as keys, so 16 bytes each

// The name of the file which will hold our filesystem
// If things get corrupted, unmount it and delete the file
// to start over with a fresh filesystem
#define DATABASE_NAME "myfs.db"

extern unqlite *pDb;

extern void error_handler(int);
void print_id(uuid_t *);

extern FILE* init_log_file();
extern void write_log(const char *, ...);

extern uuid_t zero_uuid;

// We can use the fs_state struct to pass information to fuse, which our handler functions can
// then access. In this case, we use it to pass a file handle for the file used for logging
struct myfs_state {
  FILE *logfile;
};
#define NEWFS_PRIVATE_DATA ((struct myfs_state *) fuse_get_context()->private_data)




// Some helper functions for logging etc.

// In order to log actions while running through FUSE, we have to give
// it a file handle to use. We define a couple of helper functions to do
// logging. No need to change this if you don't see a need
//

FILE *logfile;

// Open a file for writing so we can obtain a handle
FILE *init_log_file(){
  //Open logfile.
  logfile = fopen("myfs.log", "w");
  if (logfile == NULL) {
    perror("Unable to open log file. Life is not worth living.");
    exit(EXIT_FAILURE);
  }
  //Use line buffering
  setvbuf(logfile, NULL, _IOLBF, 0);
  return logfile;
}

// Write to the provided handle
void write_log(const char *format, ...){
  va_list ap;
  va_start(ap, format);
  vfprintf(NEWFS_PRIVATE_DATA->logfile, format, ap);
}

// Simple error handler which cleans up and quits
void error_handler(int rc){
  if( rc != UNQLITE_OK ){
    const char *zBuf;
    int iLen;
    unqlite_config(pDb,UNQLITE_CONFIG_ERR_LOG,&zBuf,&iLen);
    if( iLen > 0 ){
      perror("error_handler: ");
      perror(zBuf);
    }
    if( rc != UNQLITE_BUSY && rc != UNQLITE_NOTIMPLEMENTED ){
      /* Rollback */
      unqlite_rollback(pDb);
    }
    exit(rc);
  }
}

void print_id(uuid_t *id){
  size_t i; 
  for (i = 0; i < sizeof *id; i ++) {
    printf("%02x ", (*id)[i]);
  }
}

/*
  LRU Cache
 */

typedef struct
{
  union
  {
    uuid_t uuid;

    struct
    {
      uint32_t a;
      uint32_t b;
      uint32_t c;
      uint32_t d;
    };
    
  };
  
} myfs_key_t;


typedef struct _myfs_node_
{
  struct _myfs_node_ *next;
  struct _myfs_node_ *prev;

  uuid_t key;
  size_t size;
  void  *data;
  
} myfs_node_t;

myfs_node_t *myfs_mk_node(uuid_t uuid, void * data, size_t size)
{
  myfs_node_t * node = calloc(1, sizeof(myfs_node_t));

  node->size = size;

  node->data = calloc(1, size);
  memcpy(node->data, data, size);

  uuid_copy(node->key, uuid);

  return node;
}

myfs_node_t *myfs_mk_root()
{
  myfs_node_t * node = calloc(1, sizeof(myfs_node_t));
  node->next = node->prev = node;

  return node;
}

void myfs_rm_node(myfs_node_t *node)
{
  free(node->data);
  free(node);
}

void myfs_queue_top(myfs_node_t *root, myfs_node_t *node)
{
  node->next = root->next;
  node->prev = root;

  root->next->prev = node;

  root->next = node;
    
}

void myfs_queue_end(myfs_node_t *root, myfs_node_t *node)
{
  node->prev = root->prev;
  node->next = root;

  root->prev->next = node;

  root->prev = node;
}


void myfs_queue_rem(myfs_node_t *node)
{
  myfs_node_t *next = node->next;
  myfs_node_t *prev = node->prev;

  if (next)
    next->prev = prev;

  if (prev)
    prev->next = next;
}


#define TABLE_SIZE 10

#define CACHE_EVICT_SIZE 30

typedef struct _bucket_
{
  struct _bucket_ *next;
  struct _bucket_ *prev;

  myfs_node_t *node;
  
} bucket_t;

void myfs_bucket_add(bucket_t *root, bucket_t *node)
{
  node->next = root->next;
  node->prev = root;

  root->next->prev = node;

  root->next = node;
}

void myfs_bucket_rem(bucket_t *node)
{
  bucket_t *next = node->next;
  bucket_t *prev = node->prev;

  if (next)
    next->prev = prev;

  prev->next = next;
  
  free(node);
}


typedef struct
{
  int s;
  bucket_t buckets[TABLE_SIZE];

} myfs_hashtable_t;

/* 
   A hash function for uuid's adapted from https://stackoverflow.com/questions/2253693/
 */
unsigned int hash(uuid_t uuid)
{
  myfs_key_t key = {0};
  uuid_copy(key.uuid, uuid);

  return key.a ^ key.b ^ key.c ^ key.d;
}

bucket_t *mk_bucket(myfs_node_t * node)
{
  bucket_t *bucket = calloc(1, sizeof(bucket_t));
  bucket->node = node;
  return bucket;
}

myfs_hashtable_t * myfs_mk_hashtable()
{
  myfs_hashtable_t * hashtable = calloc(1, sizeof(myfs_hashtable_t));
  
  for (int i = 0; i < TABLE_SIZE; i++)
    hashtable->buckets[i].next = hashtable->buckets[i].prev = &(hashtable->buckets[i]);

   return hashtable;
}

void myfs_hashtable_put(myfs_hashtable_t *hashtable, myfs_node_t *node)
{
  
  bucket_t *n = &(hashtable->buckets[hash(node->key) % TABLE_SIZE]);
  bucket_t *b = mk_bucket(node);

  myfs_bucket_add(n, b);

  hashtable->s++;
  
}

myfs_node_t *myfs_hashtable_get(myfs_hashtable_t *hashtable, uuid_t key)
{
  bucket_t *n = &(hashtable->buckets[hash(key) % TABLE_SIZE]);
  bucket_t *c = n;

  while((c = c->next) != n) {

    if (uuid_compare(c->node->key, key) == 0)
      return c->node;
  }

  return NULL;
}

void myfs_hashtable_del(myfs_hashtable_t *hashtable, uuid_t key)
{
  bucket_t *n = &(hashtable->buckets[hash(key) % TABLE_SIZE]);
  bucket_t *c = n;

  while((c = c->next) != n) {

    if (uuid_compare(c->node->key, key) == 0) {
      hashtable->s--;

      myfs_bucket_rem(c);
      
      return;
    }
    
  }

}

void cache_evict(myfs_hashtable_t *hashtable, myfs_node_t *root, myfs_node_t *to_be_evicted)
{
  myfs_hashtable_del(hashtable, to_be_evicted->key);
  myfs_queue_rem(to_be_evicted);
  myfs_rm_node(to_be_evicted);
}

int db_put(uuid_t key, void *data, size_t size);
int db_get(uuid_t key, void *data, unqlite_int64 size);

void flush_cache(myfs_node_t *root)
{
  myfs_node_t *c = root;
  
  while((c = c->next) != root)
    db_put(c->key, c->data, c->size);
    
}
