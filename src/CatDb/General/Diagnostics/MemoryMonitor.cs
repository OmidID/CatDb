using System.Diagnostics;

namespace CatDb.General.Diagnostics
{
    public class MemoryMonitor
    {
        private readonly Process _process;
        private Task _worker;
        private bool _shutDown;

        private long _peakPagedMemorySize64;
        private long _peakWorkingSet64;
        private long _peakVirtualMemorySize64;

        private readonly bool _monitorPagedMemorySize64;
        private readonly bool _monitorWorkingSet64;
        private readonly bool _monitorVirtualMemorySize64;
        private readonly int _monitorPeriodInMilliseconds;

        public MemoryMonitor(bool monitorPagedMemorySize64, bool monitorWorkingSet64, bool monitorVirtualMemorySize64, int monitorPeriodInMilliseconds = 500)
        {
            if (!monitorPagedMemorySize64 && !monitorWorkingSet64 && !monitorVirtualMemorySize64)
                throw new ArgumentException("At least one flag has to be true.");

            _process = Process.GetCurrentProcess();

            _monitorPagedMemorySize64 = monitorPagedMemorySize64;
            _monitorWorkingSet64 = monitorWorkingSet64;
            _monitorVirtualMemorySize64 = monitorVirtualMemorySize64;
            _monitorPeriodInMilliseconds = monitorPeriodInMilliseconds;
        }

        public MemoryMonitor(int monitorPeriodInMilliseconds = 500)
            : this(true, true, true, monitorPeriodInMilliseconds)
        {
        }

        ~MemoryMonitor()
        {
            Stop();
        }

        private void DoUpate()
        {
            _process.Refresh();

            if (_monitorPagedMemorySize64)
            {
                var pagedMemorySize64 = _process.PagedMemorySize64;
                if (pagedMemorySize64 > PeakPagedMemorySize64)
                    PeakPagedMemorySize64 = pagedMemorySize64;
            }

            if (_monitorWorkingSet64)
            {
                var workingSet64 = _process.WorkingSet64;
                if (workingSet64 > PeakWorkingSet64)
                    PeakWorkingSet64 = workingSet64;
            }

            if (_monitorVirtualMemorySize64)
            {
                var virtualMemorySize64 = _process.VirtualMemorySize64;
                if (virtualMemorySize64 > PeakVirtualMemorySize64)
                    PeakVirtualMemorySize64 = virtualMemorySize64;
            }
        }

        private void DoMonitor()
        {
            while (!_shutDown)
            {
                DoUpate();

                SpinWait.SpinUntil(() => _shutDown, _monitorPeriodInMilliseconds);
            }
        }

        public void Start()
        {
            if (_worker != null)
                Stop();

            DoUpate();

            _worker = Task.Factory.StartNew(DoMonitor, TaskCreationOptions.LongRunning);
        }

        public void Stop()
        {
            if (_worker == null)
                return;

            try
            {
                _shutDown = true;
                _worker.Wait();
            }
            finally
            {
                _shutDown = false;
                _worker = null;
            }
        }

        public void Reset()
        {
            PeakPagedMemorySize64 = 0;
            PeakWorkingSet64 = 0;
            PeakVirtualMemorySize64 = 0;
        }

        public long PeakPagedMemorySize64
        {
            get => Interlocked.Read(ref _peakPagedMemorySize64);
            private set => Interlocked.Exchange(ref _peakPagedMemorySize64, value);
        }

        public long PeakWorkingSet64
        {
            get => Interlocked.Read(ref _peakWorkingSet64);
            private set => Interlocked.Exchange(ref _peakWorkingSet64, value);
        }

        public long PeakVirtualMemorySize64
        {
            get => Interlocked.Read(ref _peakVirtualMemorySize64);
            private set => Interlocked.Exchange(ref _peakVirtualMemorySize64, value);
        }
    }
}
