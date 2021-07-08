# Pages

## Adding property on some page

If you add property on some page type, do the following checklist. Add override
method if needed, but remember to call the overrided method using `super`.

- Decrease `freeBytes` in `init()`.
- Do read/write in `_readContent()` and `_writeContent()`.
- Do copy in `_copyTo()`

(If the property is with dynamic size)

- Still decrease `freeBytes` in `init()` if added another field to store the
  size.
- Update `freeBytes` whenever the property is changed.
