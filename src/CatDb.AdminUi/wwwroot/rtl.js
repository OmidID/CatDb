// Re-stamps <html dir="rtl|ltr">. App.razor's server-rendered dir attribute was observed wrong on
// the very first response for a freshly-switched RTL culture (a request-localization/streaming-
// render timing race, same class of problem as theme.js's applyCurrent — see its comment) — this
// gets called from ThemeInitializer.razor on every layout mount with the CURRENT circuit's culture,
// which by then is reliably resolved.
export function applyDir(isRtl) {
    document.documentElement.setAttribute('dir', isRtl ? 'rtl' : 'ltr');
}
