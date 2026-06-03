// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Data;

namespace CatDb.Database;

public class XFile(ITable<IData, IData> table) : XStream(table)
{
}
