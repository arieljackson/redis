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

static RedisModuleType *TriesType;

/* ========================== Internal data structure  =======================
 * This is just a linked list of 64 bit integers where elements are inserted
 * in-place, so it's ordered. There is no pop/push operation but just insert
 * because it is enough to show the implementation of new data types without
 * making things complex. */

struct TriesTypeNode {
    int64_t value;
    struct TriesTypeNode *next;
};

struct TriesTypeObject {
    struct TriesTypeNode *head;
    size_t len; /* Number of elements added. */
};

struct TriesTypeObject *createTriesTypeObject(void) {
    struct TriesTypeObject *o;
    o = RedisModule_Alloc(sizeof(*o));
    o->head = NULL;
    o->len = 0;
    return o;
}

void TriesTypeInsert(struct TriesTypeObject *o, int64_t ele) {
    struct TriesTypeNode *next = o->head, *newnode, *prev = NULL;

    while(next && next->value < ele) {
        prev = next;
        next = next->next;
    }
    newnode = RedisModule_Alloc(sizeof(*newnode));
    newnode->value = ele;
    newnode->next = next;
    if (prev) {
        prev->next = newnode;
    } else {
        o->head = newnode;
    }
    o->len++;
}

void TriesTypeReleaseObject(struct TriesTypeObject *o) {
    struct TriesTypeNode *cur, *next;
    cur = o->head;
    while(cur) {
        next = cur->next;
        RedisModule_Free(cur);
        cur = next;
    }
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

    long long value;
    if ((RedisModule_StringToLongLong(argv[2],&value) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a signed 64 bit integer");
    }

    /* Create an empty value object if the key is currently empty. */
    struct TriesTypeObject *hto;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        hto = createTriesTypeObject();
        RedisModule_ModuleTypeSetValue(key,TriesType,hto);
    } else {
        hto = RedisModule_ModuleTypeGetValue(key);
    }

    /* Insert the new element. */
    TriesTypeInsert(hto,value);

    RedisModule_ReplyWithLongLong(ctx,hto->len);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* TRIESTYPE.RANGE key first count */
int TriesTypeRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 4) return RedisModule_WrongArity(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != TriesType)
    {
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    long long first, count;
    if (RedisModule_StringToLongLong(argv[2],&first) != REDISMODULE_OK ||
        RedisModule_StringToLongLong(argv[3],&count) != REDISMODULE_OK ||
        first < 0 || count < 0)
    {
        return RedisModule_ReplyWithError(ctx,
            "ERR invalid first or count parameters");
    }

    struct TriesTypeObject *hto = RedisModule_ModuleTypeGetValue(key);
    struct TriesTypeNode *node = hto ? hto->head : NULL;
    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    long long arraylen = 0;
    while(node && count--) {
        RedisModule_ReplyWithLongLong(ctx,node->value);
        arraylen++;
        node = node->next;
    }
    RedisModule_ReplySetArrayLength(ctx,arraylen);
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

void *TriesTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != 0) {
        /* RedisModule_Log("warning","Can't load data with version %d", encver);*/
        return NULL;
    }
    uint64_t elements = RedisModule_LoadUnsigned(rdb);
    struct TriesTypeObject *hto = createTriesTypeObject();
    while(elements--) {
        int64_t ele = RedisModule_LoadSigned(rdb);
        TriesTypeInsert(hto,ele);
    }
    return hto;
}

void TriesTypeRdbSave(RedisModuleIO *rdb, void *value) {
    struct TriesTypeObject *hto = value;
    struct TriesTypeNode *node = hto->head;
    RedisModule_SaveUnsigned(rdb,hto->len);
    while(node) {
        RedisModule_SaveSigned(rdb,node->value);
        node = node->next;
    }
}

void TriesTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
    struct TriesTypeObject *hto = value;
    struct TriesTypeNode *node = hto->head;
    while(node) {
        RedisModule_EmitAOF(aof,"TRIESTYPE.INSERT","sl",key,node->value);
        node = node->next;
    }
}

/* The goal of this function is to return the amount of memory used by
 * the TriesType value. */
size_t TriesTypeMemUsage(const void *value) {
    const struct TriesTypeObject *hto = value;
    struct TriesTypeNode *node = hto->head;
    return sizeof(*hto) + sizeof(*node)*hto->len;
}

void TriesTypeFree(void *value) {
    TriesTypeReleaseObject(value);
}

void TriesTypeDigest(RedisModuleDigest *md, void *value) {
    struct TriesTypeObject *hto = value;
    struct TriesTypeNode *node = hto->head;
    while(node) {
        RedisModule_DigestAddLongLong(md,node->value);
        node = node->next;
    }
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
        .rdb_load = TriesTypeRdbLoad,
        .rdb_save = TriesTypeRdbSave,
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

    if (RedisModule_CreateCommand(ctx,"triestype.range",
        TriesTypeRange_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"triestype.len",
        TriesTypeLen_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
