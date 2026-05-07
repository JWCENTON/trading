export type ThemeMode = "dark" | "light";

const STORAGE_KEY = "waltrade-theme";

export function getInitialTheme(): ThemeMode {
  const stored = localStorage.getItem(STORAGE_KEY);

  if (stored === "dark" || stored === "light") {
    return stored;
  }

  if (window.matchMedia("(prefers-color-scheme: light)").matches) {
    return "light";
  }

  return "dark";
}

export function applyTheme(theme: ThemeMode) {
  document.documentElement.setAttribute("data-theme", theme);
  localStorage.setItem(STORAGE_KEY, theme);
}

export function toggleTheme(current: ThemeMode): ThemeMode {
  return current === "dark" ? "light" : "dark";
}
