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

/* COPY AND PASTED FROM INTERNET */
/* TODO: paste in which website u got it from */

#define ARRAY_SIZE(a) sizeof(a)/sizeof(a[0])
// Alphabet size (# of symbols)
#define ALPHABET_SIZE (26)
// Converts key current character into index
// use only 'a' through 'z' and lower case
#define CHAR_TO_INDEX(c) ((int)c - (int)'a')

static RedisModuleType *TriesType;

/* ========================== Internal data structure  =======================
 * This is just a linked list of 64 bit integers where elements are inserted
 * in-place, so it's ordered. There is no pop/push operation but just insert
 * because it is enough to show the implementation of new data types without
 * making things complex. */

struct TriesTypeNode {
    struct TriesTypeNode *children[ALPHABET_SIZE];
    bool isEndOfWord;
};

/* can change this struct if helpful later on */
struct TriesTypeObject {
    struct TriesTypeNode *root;
    size_t len; /* Number of letters (?) added. */
};

/* Create single trie node */
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

/* Create triestype object */
struct TriesTypeObject *createTriesTypeObject(void) {
    struct TriesTypeObject *o;
    struct TriesTypeNode *node;
    o = RedisModule_Alloc(sizeof(*o));
    node = createTriesTypeNode();
    o->root = node;
    o->len = 0;
    return o;
}

// If not present, inserts key into trie
// If the key is prefix of trie node, just marks leaf node
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

// Returns true if key presents in trie, else false
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

/* TODO: this function doesn't work */
void TriesTypeReleaseObject(struct TriesTypeObject *o) {
    struct TriesTypeNode *cur;
    cur = o->root;
    //TODO: recursive free the tree!
    RedisModule_Free(cur);
    RedisModule_Free(o);
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
/* Prints out 1 if true, 0 if false */
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

    /* Insert the new element. */
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


/* ========================== "triestype" type methods ======================= */

/* ALL OF THIS IS WRONG */
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

// bullshit bullshit bullshit
void TriesTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    struct TriesTypeObject *hto = value;
    struct TriesTypeNode *node = hto->root;
    while(node) {
        RedisModule_EmitAOF(aof,"TRIESTYPE.INSERT","sl",key, hto->len);
    }
}

/* The goal of this function is to return the amount of memory used by
 * the TriesType value. */
size_t TriesTypeMemUsage(const void *value) {
    const struct TriesTypeObject *hto = value;
    struct TriesTypeNode *node = hto->root;
    return sizeof(*hto) + sizeof(*node)*hto->len;
}

void TriesTypeFree(void *value) {
    TriesTypeReleaseObject(value);
}

void TriesTypeDigest(RedisModuleDigest *md, void *value) {
    struct TriesTypeObject *hto = value;
    // this is complete bullshit, im literally adding length
    // as the size  of memory
    // just doing this to get the compiler to shut up
    RedisModule_DigestAddLongLong(md,hto->len);
    RedisModule_DigestEndSequence(md);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
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

    return REDISMODULE_OK;
}
