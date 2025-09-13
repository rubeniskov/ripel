use core::mem::MaybeUninit;
use anyhow::Result;

use crate::entity::EntityModel;

/// The type stored per entry.
#[repr(transparent)]
pub struct Entry(pub fn() -> &'static EntityModel);

// Anchor arrays to mark start/end of the section.
// Names chosen to be unique and stable.
#[used]
#[cfg_attr(any(target_os = "linux", target_os = "android"), unsafe(link_section = ".ripel_entities$a"))]
#[cfg_attr(target_os = "macos", unsafe(link_section = "__DATA,__ripel_entities"))]
#[cfg_attr(windows, unsafe(link_section = ".ripel_entities$a"))]
pub static __RIPEL_ENTITIES_START: [MaybeUninit<Entry>; 0] = [];

#[used]
#[cfg_attr(any(target_os = "linux", target_os = "android"), unsafe(link_section = ".ripel_entities$z"))]
#[cfg_attr(target_os = "macos", unsafe(link_section = "__DATA,__ripel_entities"))]
#[cfg_attr(windows, unsafe(link_section = ".ripel_entities$z"))]
pub static __RIPEL_ENTITIES_END: [MaybeUninit<Entry>; 0] = [];

/// # Safety
/// Must be called only after the image is loaded (normal at runtime).
pub fn all_models() -> impl Iterator<Item = &'static EntityModel> {
    // SAFETY: start/end are in the same section; compute raw span.
    unsafe {
        let start = __RIPEL_ENTITIES_START.as_ptr() as *const Entry;
        let end   = __RIPEL_ENTITIES_END.as_ptr()   as *const Entry;
        let len = (end as usize - start as usize) / core::mem::size_of::<Entry>();
        let slice = core::slice::from_raw_parts(start, len);
        slice.iter().map(|e| (e.0)())
    }
}

/// Helper macro for consumers; no external crate needed.
#[macro_export]
macro_rules! register_entity {
    ($f:expr) => {
        #[used]
        #[cfg_attr(any(target_os = "linux", target_os = "android"), link_section = ".ripel_entities$m")]
        #[cfg_attr(target_os = "macos", link_section = "__DATA,__ripel_entities")]
        #[cfg_attr(windows, link_section = ".ripel_entities$m")]
        static __RIPEL_ENTITY_ENTRY: $crate::registry::Entry = $crate::registry::Entry($f);
    };
}

pub fn get_entity_by_table_name(table_name: &str) -> Result<&'static EntityModel> {
    all_models().find(|m| m.table_name == table_name).ok_or_else(|| {
        anyhow::anyhow!("Entity with table name `{}` not found", table_name)
    })
}

pub fn get_entity_by_name(name: &str) -> Result<&'static EntityModel> {
    all_models().find(|m| m.entity_name == name).ok_or_else(|| {
        anyhow::anyhow!("Entity with name `{}` not found", name)
    })
}
