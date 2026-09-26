# Copal brand assets

The Copal mark is a drop of resin laid down in translucent layers. Each
layer covers everything beneath it, so the drop gets denser toward the
bottom, the way versions accumulate on a file. Copal is the young resin
that becomes amber, and preserving things is what this service does.

The treatment is hueless (the "Smoke" palette): warm near-black and
off-white, with the strata built from one color at increasing opacity.

## Files

| File | Size | Use it for |
| --- | --- | --- |
| `banner.png` | 2560x1280 | The header image at the top of the README. |
| `social-preview.png` | 1280x640 | The GitHub social preview (see below). Also works for link cards and slides. |
| `icon.svg` | 512x512 | The app icon: the mark on a dark rounded tile. Use it for avatars, package registries, and anywhere a square icon is needed. |
| `icon-512.png` | 512x512 | The same icon as a PNG, for places that do not accept SVG. |
| `favicon-32.png` | 32x32 | A browser tab icon. |
| `mark-dark.svg` | 512x512 | The bare mark for dark backgrounds. The background is transparent. |
| `mark-light.svg` | 512x512 | The bare mark for light backgrounds. The background is transparent. |
| `lockup-dark.png` | 1384x352 | The mark with the `copal` wordmark on a dark tile. |
| `lockup-light.png` | 1384x352 | The mark with the `copal` wordmark on a light tile. |

To show the right bare mark for the reader's GitHub theme:

```html
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/mark-dark.svg">
  <img src="assets/mark-light.svg" alt="Copal" width="96">
</picture>
```

## Colors

| Role | Dark | Light |
| --- | --- | --- |
| Ground | `#1A1410` | `#F1EDE6` |
| Strata (five layers at 34, 52, 68, 83, and 96 percent cumulative opacity) | `#EFE7D8` | `#2A1B0E` |
| Wordmark | `#F4EADA` | `#2A1B0E` |

The wordmark is set in Source Sans Pro, weight 600.

## Social preview

GitHub does not read the social preview from the repository. To set it,
open the repository's Settings, and under General > Social preview, upload
`social-preview.png`.

## Source

The artwork is drawn in Penpot, in the Brands project, file "Repository
Brands". The mark is the "copal mark" component with Concept "C4 Strata
Drop" and Hue "Smoke". The banner is the copal README banner board on the
Family page. Export from there if you need another size or format.
