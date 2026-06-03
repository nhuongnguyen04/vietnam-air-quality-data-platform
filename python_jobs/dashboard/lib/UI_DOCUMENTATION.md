# 🎨 UI & Styling Library Architecture Documentation

This document explains the separation of concerns, responsibilities, and guidelines for the 5 dashboard UI and styling files located under `lib/`.

---

## 📋 File Responsibilities Matrix

| File | Primary Concern | Common Use Case | Key Functions / Constants |
|---|---|---|---|
| **[style.py](file:///home/nhuong/vietnam-air-quality-data-platform/python_jobs/dashboard/lib/style.py)** | Global CSS Injections | Theme-aware layout styling, Sidebar styling, custom scrollbars | `inject_style()` |
| **[design_tokens.py](file:///home/nhuong/vietnam-air-quality-data-platform/python_jobs/dashboard/lib/design_tokens.py)** | Theme Variables | Exposing raw colors, borders, and shadows | `get_theme_tokens()` |
| **[page_helpers.py](file:///home/nhuong/vietnam-air-quality-data-platform/python_jobs/dashboard/lib/page_helpers.py)** | Page Layout scaffolding | Standard headers, loading skeletons, error boundaries, page decorator | `page_wrapper`, `render_unified_brand_header`, `render_loading_skeleton` |
| **[ui_components.py](file:///home/nhuong/vietnam-air-quality-data-platform/python_jobs/dashboard/lib/ui_components.py)** | Custom reusable widgets | KPI cards, maps, distribution and ranking charts | `render_kpi_card`, `render_map_component`, `render_progress_bar` |
| **[chart_config.py](file:///home/nhuong/vietnam-air-quality-data-platform/python_jobs/dashboard/lib/chart_config.py)** | Plotly specific presets | Color palettes, empty state graphs, custom height parameters | `get_plotly_layout`, `create_empty_state`, `SOURCE_PALETTE` |

---

## 🔍 Detailed Specifications

### 1. `style.py`
- **Purpose**: Wires up the CSS layer. Loads external fonts (Outfit, Plus Jakarta Sans, JetBrains Mono, Material Symbols).
- **Guidelines**: Do not define query or data fetching logic here. If you need a new CSS class or selector override, place it here within `inject_style()`.

### 2. `design_tokens.py`
- **Purpose**: The design system's source of truth for colors and layout properties.
- **Guidelines**: When writing raw HTML style strings inside widgets, *always* retrieve parameters using `get_theme_tokens()`. This ensures absolute compatibility across light and dark mode toggles.

### 3. `page_helpers.py`
- **Purpose**: Page-wide decorator hooks and layout scaffolding wrappers.
- **Guidelines**: Every Streamlit subpage should be decorated with `@page_wrapper` which automatically runs theme injection, renders the unified brand header, and manages locale-aware error boundaries.

### 4. `ui_components.py`
- **Purpose**: Interactive and visual elements that render charts or HTML components.
- **Guidelines**: High-fidelity widgets like KPI metric cards should be constructed using standard HTML/CSS blocks inside `render_kpi_card` for precise layout control.

### 5. `chart_config.py`
- **Purpose**: Centralizes Plotly configuration and presets.
- **Guidelines**: Always apply `get_plotly_layout()` to newly created figures to maintain matching background colors, grid lines, and fonts. Use `create_empty_state()` for pages with missing dataset selections.
