// ── Dashboard entry point ──────────────────────
// Imports the functions referenced by inline HTML handlers and exposes them on
// window.App (e.g. App.switchView(...)), then pulls in the side-effect modules
// (loading kicks off data fetch on DOMContentLoaded; search + events attach
// their listeners).
import { switchView, handleMonthChange, stepMonth, positionViewSlider } from './views.js';
import { toggleCategoryView, toggleGoalEdit } from './charts_overview.js';
import { changeGoldTimeframe } from './gold.js';
import { closePhotoLightbox, openPhotoLightbox, togglePanelPhotos } from './memories.js';
import { closeSidePanel, toggleOutliers } from './outliers.js';
import {
    clearSelection, onPanelFilterChange, setPanelSort,
    showCategoryDetails, showDayDetails, toggleTxSelection, calStepMonth,
} from './panel.js';
import { handleSort, loadMoreTransactions, toggleChart } from './transactions.js';
import { initOverrideDelegation } from './overrides.js';
import { openNewCategoryDialog, openEditCategoryDialog, deleteCategory } from './categories.js';
import { setDailyMode } from './charts_monthly.js';
import { toggleTheme, initTheme } from './theme.js';
import { toggleSidebar, initSidebar } from './sidebar.js';
import { initOnboarding } from './onboarding.js';
import { loadAllData } from './loading.js';

// Side-effect modules: register their DOM listeners / startup behaviour.
import './loading.js';
import './search.js';
import './events.js';

// One delegated click listener handles every editable category badge.
initOverrideDelegation();
// Sync the theme toggle icon with the pre-paint theme.
initTheme();
// Place the segmented-switch slider under the active tab (and keep it there on resize).
positionViewSlider();
window.addEventListener('load', positionViewSlider);
window.addEventListener('resize', positionViewSlider);
// Sidebar (data & settings drawer).
initSidebar();
// Onboarding / empty-state prompt (first run with no data).
initOnboarding();

window.App = {
    switchView, handleMonthChange, stepMonth,
    toggleCategoryView,
    changeGoldTimeframe,
    closePhotoLightbox, openPhotoLightbox, togglePanelPhotos,
    closeSidePanel, toggleOutliers,
    clearSelection, onPanelFilterChange, setPanelSort,
    showCategoryDetails, showDayDetails, toggleTxSelection, calStepMonth,
    handleSort, loadMoreTransactions, toggleChart,
    setDailyMode, toggleGoalEdit,
    toggleTheme, toggleSidebar,
    // No tx to move when opened from the breakdown; just refresh so the new category shows.
    openNewCategoryDialog: () => openNewCategoryDialog(() => loadAllData()),
    // Edit / delete a custom category, then refresh the whole dashboard.
    editCategory: (name) => openEditCategoryDialog(name, () => loadAllData()),
    deleteCategory: (name) => deleteCategory(name, () => loadAllData()),
};
