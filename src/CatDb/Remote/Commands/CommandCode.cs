// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.Remote.Commands;
public static class CommandCode
{
    public const int UNDEFINED = 0;

    // XTable
    public const int REPLACE = 1;
    public const int DELETE = 2;
    public const int DELETE_RANGE = 3;
    public const int INSERT_OR_IGNORE = 4;
    public const int CLEAR = 5;

    public const int TRY_GET = 6;
    public const int FORWARD = 7;
    public const int BACKWARD = 8;
    public const int FIND_NEXT = 9;
    public const int FIND_AFTER = 10;
    public const int FIND_PREV = 11;
    public const int FIND_BEFORE = 12;
    public const int FIRST_ROW = 13;
    public const int LAST_ROW = 14;
    public const int COUNT = 15;

    public const int XTABLE_DESCRIPTOR_GET = 16;
    public const int XTABLE_DESCRIPTOR_SET = 17;

    // Index operations (on XTable)
    public const int INDEX_CREATE = 18;
    public const int INDEX_DROP = 19;
    public const int INDEX_FIND = 20;
    public const int INDEX_EXISTS = 21;

    // Storage engine
    public const int STORAGE_ENGINE_COMMIT = 22;
    public const int STORAGE_ENGINE_GET_ENUMERATOR = 23;
    public const int STORAGE_ENGINE_RENAME = 24;
    public const int STORAGE_ENGINE_EXISTS = 25;
    public const int STORAGE_ENGINE_FIND_BY_NAME = 26;
    public const int STORAGE_ENGINE_FIND_BY_ID = 27;
    public const int STORAGE_ENGINE_OPEN_XTABLE = 28;
    public const int STORAGE_ENGINE_OPEN_XFILE = 29;
    public const int STORAGE_ENGINE_DELETE = 30;
    public const int STORAGE_ENGINE_COUNT = 31;
    public const int STORAGE_ENGINE_DESCRIPTOR = 32;
    public const int STORAGE_ENGINE_GET_CACHE_SIZE = 33;
    public const int STORAGE_ENGINE_SET_CACHE_SIZE = 34;

    // Index additional operations
    public const int INDEX_FIND_RANGE = 35;
    public const int INDEX_COUNT = 36;
    public const int INDEX_REBUILD = 37;
    public const int INDEX_LIST = 38;
    public const int INDEX_FIND_PREFIX = 39;
    public const int INDEX_QUERY = 51;
    public const int INDEX_COUNT_QUERY = 52;
    public const int RANGE_COUNT = 53;

    //Heap
    public const int HEAP_OBTAIN_NEW_HANDLE = 40;
    public const int HEAP_RELEASE_HANDLE = 41;
    public const int HEAP_EXISTS_HANDLE = 42;
    public const int HEAP_WRITE = 43;
    public const int HEAP_READ = 44;
    public const int HEAP_COMMIT = 45;
    public const int HEAP_CLOSE = 46;
    public const int HEAP_GET_TAG = 47;
    public const int HEAP_SET_TAG = 48;
    public const int HEAP_DATA_SIZE = 49;
    public const int HEAP_SIZE = 50;

    public const int EXCEPTION = 63;
    public const int MAX = 80;
}
