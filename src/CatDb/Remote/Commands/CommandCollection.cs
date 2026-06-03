// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.General.Extensions;

namespace CatDb.Remote.Commands;
public class CommandCollection : List<ICommand>
{
    public bool AreAllCommon { get; private set; }
    public int CommonAction { get; private set; }

    public CommandCollection(ICommand[] operations, bool areAllCommon, int commonCode)
    {
        this.SetArray(operations);

        AreAllCommon = areAllCommon;
        CommonAction = commonCode;
    }

    public CommandCollection(int capacity)
        : base(capacity)
    {
        AreAllCommon = true;
        CommonAction = CommandCode.UNDEFINED;
    }

    public new void Add(ICommand command)
    {
        if (AreAllCommon)
        {
            if (Count == 0)
                CommonAction = command.Code;

            if (command.Code != CommonAction)
            {
                AreAllCommon = false;
                CommonAction = CommandCode.UNDEFINED;
            }
        }

        base.Add(command);
    }

    public new ICommand this[int index] => base[index];

    public new void Clear()
    {
        base.Clear();

        AreAllCommon = true;
        CommonAction = CommandCode.UNDEFINED;
    }
}
