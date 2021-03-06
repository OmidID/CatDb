namespace CatDb.General.IO
{
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
}
