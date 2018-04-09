/* This file implements a new module native data type called "TRIESTYPE".
 * The data structure implemented is a very simple ordered linked list of
 * 64 bit integers, in order to have something that is real world enough, but
 * at the same time, extremely simple to understand, to show how the API
 * works, how a new data type is created, and how to write basic methods
 * for RDB loading, saving and AOF rewriting.
 *
 * -----------------------------------------------------------------------------
 *
 * Copyright (c) 2016, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "../redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>

/* from https://www.geeksforgeeks.org/trie-insert-and-search/ */

#define ARRAY_SIZE(a) sizeof(a)/sizeof(a[0])
// Alphabet size (# of symbols)
#define ALPHABET_SIZE (26)
// Converts key current character into index
// use only 'a' through 'z' and lower case
#define CHAR_TO_INDEX(c) ((int)c - (int)'a')

static RedisModuleType *TriesType;

/* ========================== Internal data structure  =======================
 * Implements a trie, with a 26 letter alphabet.
 */

/* Linkedlist structs */
struct listNode {
   char *word;
   struct listNode *next;
};

struct listHead {
    struct listNode *head;
};

/* Trie structs */
struct TriesTypeNode {
    struct TriesTypeNode *children[ALPHABET_SIZE];
    bool isEndOfWord;
};

struct TriesTypeObject {
    struct TriesTypeNode *root;
    size_t len; /* Number of words added. */
};


/* Linkedlist functions */
/* listInsert: insert at the first location in list */
void listInsert(struct listHead *head, char *word) {
    if (!head->head) {
        struct listNode *link = (struct listNode*) RedisModule_Alloc(sizeof(struct listNode));
        link->word = word;
        link->next = NULL;
        head->head = link;
        return;
    }
   //create a link
   struct listNode *link = (struct listNode*) RedisModule_Alloc(sizeof(struct listNode));
   link->word = word;
   link->next = head->head;
   head->head = link;
   return;
}

/* createTriesTypeNode: create single trie node, uses redis allocator */
struct TriesTypeNode *createTriesTypeNode(void)
{
    struct TriesTypeNode *node;
    node = RedisModule_Alloc(sizeof(*node));
    if (node)
    {
        int i;
        node->isEndOfWord = false;
        for (i = 0; i < ALPHABET_SIZE; i++)
            node->children[i] = NULL;
    }
    return node;
}

/* createTriesTypeObject: Create triestype object, with single root node */
struct TriesTypeObject *createTriesTypeObject(void) {
    struct TriesTypeObject *o;
    struct TriesTypeNode *node;
    o = RedisModule_Alloc(sizeof(*o));
    node = createTriesTypeNode();
    o->root = node;
    o->len = 0;
    return o;
}

/* TriesTypeInsert:  If not present, inserts string key into trie
 *  If the key is prefix of trie node, just marks leaf node */
void TriesTypeInsert(struct TriesTypeObject *o, const char *key, size_t length) {
    unsigned int level;
    unsigned int index;
 
    struct TriesTypeNode *pCrawl = o->root;
 
    for (level = 0; level < length; level++)
    {
        index = CHAR_TO_INDEX(key[level]);
        if (!pCrawl->children[index]) {
           
            pCrawl->children[index] = createTriesTypeNode();
        }
        pCrawl = pCrawl->children[index];
    }
 
    // mark last node as leaf
    // Only increase length if we actually insert a word???
    if (pCrawl->isEndOfWord != true) {
        o->len++;
    } 
    pCrawl->isEndOfWord = true;
}

/* TriesTypeSearch:  Returns true if key presents in trie, else false */
bool TriesTypeSearch(struct TriesTypeObject *o, const char *key, size_t length)
{
    unsigned int level;
    unsigned int index;
    struct TriesTypeNode *pCrawl = o->root;
 
    for (level = 0; level < length; level++)
    {
        index = CHAR_TO_INDEX(key[level]);
 
        if (!pCrawl->children[index])
            return false;
 
        pCrawl = pCrawl->children[index];
    }
 
    return (pCrawl != NULL && pCrawl->isEndOfWord);
}


/* isLastNode: checks if the current node has any children
 * Returns 0 if current node has a child
 * If all children are NULL, return 1.
 */
bool isLastNode(struct TriesTypeNode* root)
{
    for (int i = 0; i < ALPHABET_SIZE; i++)
        if (root->children[i])
            return 0;
    return 1;
}

// Recursive function to print auto-suggestions for given
// node.
void suggestionsRec(struct TriesTypeNode* root, const char *currPrefix, struct listHead *head)
{

    // found a string in Trie with the given prefix
    if (root->isEndOfWord)
    {
        listInsert(head, (char *) currPrefix);
        printf("%s\n", head->head->word);
        // printf("%s\n", currPrefix);
    }
 
    // All children struct node pointers are NULL
    if (isLastNode(root))
        return;
 
    for (int i = 0; i < ALPHABET_SIZE; i++)
    {
        if (root->children[i])
        {
            // append current character to currPrefix string
            char *new_prefix ;
            if((new_prefix = RedisModule_Alloc(strlen(currPrefix)+2)) != NULL){
                // memset(new_prefix, 0);   // ensures the memory is an empty string
                strcat(new_prefix,currPrefix);
                char str[2] = "\0";
                str[0] = (char) (97+i);
                strcat(new_prefix, str);
                // printf("new_prefix: %s\n", new_prefix);
            } else {
                // fprintf(STDERR,"malloc failed!\n");
                return;
            }

            // recur over the rest
            suggestionsRec(root->children[i], new_prefix, head);
        }
    }
}

/* TriesTypeSuffix: Given a prefix, returns a list of corresponding words in the trie
 *  TODO If the prefix is "*", returns all the words in the trie */
struct listHead *TriesTypeSuffix(struct TriesTypeObject *o, const char *key, size_t length) {
    unsigned int level;
    unsigned int index;
 
    struct TriesTypeNode *pCrawl = o->root;
 
    for (level = 0; level < length; level++)
    {
        index = CHAR_TO_INDEX(key[level]);
        if (!pCrawl->children[index]) {
           return NULL;
        }
        pCrawl = pCrawl->children[index];
    }
 
    // If prefix is present as a word.
    bool isWord = (pCrawl->isEndOfWord == true);
 
    // If prefix is last node of tree (has no
    // children)
    bool isLast = isLastNode(pCrawl);
 
    // If prefix is present as a word, but
    // there is no subtree below the last
    // matching node.
    if (isWord && isLast)
    {
        struct listHead *head = malloc(sizeof(struct listNode));
        head->head = NULL;
        listInsert(head, (char *)key);
        return head;
    }
 
    // If there are are nodes below last
    // matching character.
    if (!isLast)
    {
        const char *prefix = key;
        struct listHead *head = malloc(sizeof(struct listNode));
        head->head = NULL;
        suggestionsRec(pCrawl, prefix, head);
        // if (head->head) {
        //     struct listNode *tmp = head->head;
        //     while (tmp) {
        //         printf("%s\n", tmp->word);
        //         tmp = tmp->next;
        //     }
        // }
        return head;
    }
    return NULL;

}


void TriesTypeReleaseNode(struct TriesTypeNode *node)
{
    int i;
    if (!node) {
        return;   // safe guard including root node. 
    }
    // recursive case (go to end of trie)
    for (i = 0; i < ALPHABET_SIZE; i++) {
        if (node->children[i]) {
            TriesTypeReleaseNode(node->children[i]);
        }
    }
    // base case
    RedisModule_Free(node);
}

/*  TriesTypeReleaseObject: frees the tries type object */
void TriesTypeReleaseObject(struct TriesTypeObject *o) 
{
    if (o) {
        if (o->root) {
            TriesTypeReleaseNode(o->root);
        }
        RedisModule_Free(o);
    }
}

/* ========================= "triestype" type commands ======================= */

/* TRIESTYPE.INSERT key value */
int TriesTypeInsert_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != TriesType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    const char *value ;
    size_t len;
    value = RedisModule_StringPtrLen(argv[2], &len);

    /* Create an empty value object if the key is currently empty. */
    struct TriesTypeObject *hto;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        hto = createTriesTypeObject();
        RedisModule_ModuleTypeSetValue(key,TriesType,hto);
    } else {
        hto = RedisModule_ModuleTypeGetValue(key);
    }

    /* Insert the new element. */
    TriesTypeInsert(hto,value, len);

    RedisModule_ReplyWithLongLong(ctx,hto->len);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* TRIESTYPE.SEARCH key value */
/* Prints out YES if true, NO if false */
int TriesTypeSearch_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != TriesType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    const char *value ;
    size_t len;
    value = RedisModule_StringPtrLen(argv[2], &len);

    /* Create an empty value object if the key is currently empty. */
    struct TriesTypeObject *hto;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        hto = createTriesTypeObject();
        RedisModule_ModuleTypeSetValue(key,TriesType,hto);
    } else {
        hto = RedisModule_ModuleTypeGetValue(key);
    }

    /* Search for element */
    bool check = TriesTypeSearch(hto,value, len);
    if (check) {
        RedisModule_ReplyWithSimpleString(ctx, "YES");
    }
    else {
        RedisModule_ReplyWithSimpleString(ctx, "NO");
    }
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* TRIESTYPE.LEN key */
int TriesTypeLen_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 2) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != TriesType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    struct TriesTypeObject *hto = RedisModule_ModuleTypeGetValue(key);
    RedisModule_ReplyWithLongLong(ctx,hto ? hto->len : 0);
    return REDISMODULE_OK;
}

/* TRIESTYPE.SEARCH key suffix */
int TriesTypeSuffix_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 3) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != TriesType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    const char *value ;
    size_t len;
    value = RedisModule_StringPtrLen(argv[2], &len);

    // long long count;
    // if ( RedisModule_StringToLongLong(argv[3],&count) != REDISMODULE_OK ||
    //     count < 0)
    // {
    //     return RedisModule_ReplyWithError(ctx,
    //         "ERR invalid first or count parameters");
    // }

    struct TriesTypeObject *tto = RedisModule_ModuleTypeGetValue(key);
    // struct TriesTypeNode *node = tto ? tto->root : NULL;



    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    long long arraylen = 0;
    struct listHead *list = TriesTypeSuffix(tto,value,len);
    struct listNode *head = list? list->head : NULL;
    while(head) {
        RedisModule_ReplyWithSimpleString(ctx, head->word);
        arraylen++;
        head = head->next;
    }
    RedisModule_ReplySetArrayLength(ctx,arraylen);
    return REDISMODULE_OK;
}



/* ========================== "triestype" type methods ======================= */

/* ALL OF THIS IS BROKEN */
// void *TriesTypeRdbLoad(RedisModuleIO *rdb, int encver) {
//     if (encver != 0) {
//         /* RedisModule_Log("warning","Can't load data with version %d", encver);*/
//         return NULL;
//     }
//     uint64_t elements = RedisModule_LoadUnsigned(rdb);
//     struct TriesTypeObject *hto = createTriesTypeObject();
//     while(elements--) {
//         int64_t ele = RedisModule_LoadSigned(rdb);
//         TriesTypeInsert(hto,ele);
//     }
//     return hto;
// }

// void TriesTypeRdbSave(RedisModuleIO *rdb, void *value) {
//     struct TriesTypeObject *hto = value;
//     struct TriesTypeNode *node = hto->head;
//     RedisModule_SaveUnsigned(rdb,hto->len);
//     while(node) {
//         RedisModule_SaveSigned(rdb,node->value);
//         node = node->next;
//     }
// }

/* THIS FUNCTION IS BROKEN AND DOES NOT SAVE CORRECTLY */
void TriesTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    struct TriesTypeObject *hto = value;
    struct TriesTypeNode *node = hto->root;
    while(node) {
        RedisModule_EmitAOF(aof,"TRIESTYPE.INSERT","sl",key, hto->len);
    }
}

/* The goal of this function is to return the amount of memory used by
 * the TriesType value.  BROKEN */
size_t TriesTypeMemUsage(const void *value) {
    const struct TriesTypeObject *hto = value;
    struct TriesTypeNode *node = hto->root;
    return sizeof(*hto) + sizeof(*node)*hto->len;
}

/* NOT BROKEN */
void TriesTypeFree(void *value) {
    TriesTypeReleaseObject(value);
}

/* BROKEN */
void TriesTypeDigest(RedisModuleDigest *md, void *value) {
    struct TriesTypeObject *hto = value;
    // Broken - literally adding length
    // to get the compiler to shut up
    RedisModule_DigestAddLongLong(md,hto->len);
    RedisModule_DigestEndSequence(md);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server.
 * NOT BROKEN. Most important function
 */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx,"triestype",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        // .rdb_load = TriesTypeRdbLoad,
        // .rdb_save = TriesTypeRdbSave,
        .aof_rewrite = TriesTypeAofRewrite,
        .mem_usage = TriesTypeMemUsage,
        .free = TriesTypeFree,
        .digest = TriesTypeDigest
    };

    TriesType = RedisModule_CreateDataType(ctx,"triestype",0,&tm);
    if (TriesType == NULL) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"triestype.insert",
        TriesTypeInsert_RedisCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"triestype.search",
        TriesTypeSearch_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"triestype.len",
        TriesTypeLen_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"triestype.suffix",
        TriesTypeSuffix_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
