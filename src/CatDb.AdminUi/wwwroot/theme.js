const STORAGE_KEY = 'catdb-theme';
const media = window.matchMedia('(prefers-color-scheme: dark)');

function systemPrefersDark() {
    return media.matches;
}

// 'system' means "no explicit override" — always follow the OS setting, including live changes.
export function getMode() {
    const stored = localStorage.getItem(STORAGE_KEY);
    return stored === 'dark' || stored === 'light' ? stored : 'system';
}

export function isDark() {
    const mode = getMode();
    return mode === 'system' ? systemPrefersDark() : mode === 'dark';
}

export function setMode(mode) {
    if (mode === 'system') {
        localStorage.removeItem(STORAGE_KEY);
    } else {
        localStorage.setItem(STORAGE_KEY, mode);
    }
    applyCurrent();
}

// Re-stamps the <html class="dark"> from the current mode. The pre-paint inline script in
// App.razor's <head> only runs once per real document load — Blazor's client-side navigation
// (e.g. MainLayout's auth redirect to /login, a plain <NavLink> click) re-renders the page without
// a real reload and was observed wiping that class. ThemeInitializer.razor calls this on every
// layout mount so the theme survives in-app navigation too.
export function applyCurrent() {
    document.documentElement.classList.toggle('dark', isDark());
}
