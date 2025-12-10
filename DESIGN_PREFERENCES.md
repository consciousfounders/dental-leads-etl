# Design Preferences

## Dashboard Aesthetic: "OpenAI Modern Dark"

### Color Palette

| Role | Color | Hex |
|------|-------|-----|
| **Background** | Deep Black | `#0d0d0d` |
| **Surface** | Dark Gray | `#1a1a1a` |
| **Border** | Subtle White | `rgba(255,255,255,0.08)` |
| **Text Primary** | Soft White | `rgba(255,255,255,0.8)` |
| **Text Secondary** | Muted | `rgba(255,255,255,0.5)` |
| **Accent** | Teal (OpenAI Green) | `#10a37f` |
| **Accent Light** | Light Teal | `#6ee7b7` |
| **Accent Dark** | Deep Teal | `#0d3d38` |

### Typography

- **Font Family:** Inter, -apple-system, sans-serif
- **Headers:** 600 weight, clean spacing
- **Body:** 400 weight, 0.8-0.9 opacity white
- **Labels:** Uppercase, letter-spacing 0.5px, smaller size

### Chart Color Scale

Single teal gradient for all data visualizations:
```python
teal_scale = [[0, '#0d3d38'], [0.5, '#10a37f'], [1, '#6ee7b7']]
```

### Component Styling

#### KPI Cards
- Uniform color (dark background with border)
- No gradients or shadows
- Large value, small label
- Subtle border: `1px solid rgba(255,255,255,0.08)`

#### Tabs
- Dark background: `rgba(255,255,255,0.05)`
- **Hover effect:** `translateY(-2px)` lift
- **Selected:** Teal background with subtle glow
- Equal sizing, no icons in production

#### Insight Boxes
- Teal left border (3px solid #10a37f)
- Semi-transparent teal background
- Used sparingly for key explanations

#### Charts
- Transparent backgrounds
- Grid lines: `rgba(255,255,255,0.06)`
- No unnecessary legends or color bars
- Text labels on bars where appropriate

### Layout Principles

1. **No sidebar** — full-width content focus
2. **Tab navigation** — clear, at top
3. **KPIs first** — summary metrics visible immediately
4. **Minimal borders** — let content breathe
5. **Consistent spacing** — use margin/padding systematically

### Anti-Patterns (Avoid)

- ❌ Rainbow/multi-color palettes
- ❌ Heavy shadows or glows
- ❌ Emojis in headers (subtle use only)
- ❌ White backgrounds or cards
- ❌ Cluttered sidebars
- ❌ Multiple competing accent colors

### Inspiration

- OpenAI dashboard aesthetic
- Linear.app
- Vercel dashboard
- Modern SaaS dark themes

---

*Design choices by Zander Ford, December 2024*

