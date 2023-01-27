export const debug_enabled = false;

export const debug_ref = debug_enabled;
export const debug_allocate = debug_enabled;
export const debug_node = debug_enabled;

export function debugLog(...args: any) {
  console.debug(...args);
}
