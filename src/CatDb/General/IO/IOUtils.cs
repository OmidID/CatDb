// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿namespace CatDb.General.IO;
public static class IoUtils
{
    public static long GetTotalFreeSpace(string driveName)
    {
        driveName = driveName.ToUpper();

        var drive = DriveInfo.GetDrives().Where(x => x.IsReady && x.Name == driveName).FirstOrDefault();

        return drive != null ? drive.TotalFreeSpace : -1;
    }

    public static long GetTotalSpace(string driveName)
    {
        driveName = driveName.ToUpper();

        var drive = new DriveInfo(driveName);

        return drive != null ? drive.TotalSize : -1;
    }
}
