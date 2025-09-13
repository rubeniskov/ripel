
use anyhow::Result;

use crate::refs::Hop;

pub trait Validate {
    fn validate(&self) -> Result<()>;
}

/// Field metadata produced by the derive macro.
#[derive(Debug)]
pub enum FieldModel {
    TableField(TableField),
    ReferenceField(ReferenceField),
}

impl std::fmt::Display for FieldModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldModel::TableField(t) => write!(f, "{}", t),
            FieldModel::ReferenceField(r) => write!(f, "{}", r),
        }
    }
}

#[derive(Debug)]
pub struct TableField {
    /// Rust field ident
    pub name: &'static str,
    /// The primary key field
    pub primary_key: bool,
    /// e.g., "Club.id"
    pub column: &'static str,      
    pub template: Option<&'static str>,
    pub ty_name: &'static str,
    pub nullable: bool,
}

impl std::fmt::Display for TableField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.ty_name)?;
        if self.primary_key {
            write!(f, " [PK]")?;
        }
        write!(f, " @{}", self.column)?;
        if let Some(t) = self.template {
            write!(f, " {{tpl: {}}}", t)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ReferenceField {
    pub name: &'static str,         
    pub reference: &'static str,    
    pub via:        &'static [Hop<'static>], 
    pub ty_name: &'static str,      
}

impl std::fmt::Display for ReferenceField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.ty_name)?;
        write!(f, " (ref {})", self.reference)?;
        if !self.via.is_empty() {
            write!(f, " {{via: {:?}}}", self.via)?;
        }
        Ok(())
    }
}

/// Entity metadata produced by the derive macro.
#[derive(Debug)]
pub struct EntityModel {
    pub entity_name: &'static str, // logical "domain" name (e.g., "Player")
    pub table_name:  &'static str, // DB table (e.g., "Jugador")
    pub rust_name:   &'static str, // Rust type name
    pub fields:      &'static [FieldModel],
    pub primary_key: &'static str, // name of the PK field
}

impl EntityModel {
    /// Lookup a field model by Rust field name.
    pub fn field(&'static self, name: &str) -> Option<&'static FieldModel> {
        self.fields.iter().find(|f| match f {
            FieldModel::TableField(t) => t.name == name,
            FieldModel::ReferenceField(r) => r.name == name,
        })
    }
}

pub struct EntityAny {
    pub model: &'static EntityModel,
    pub value: Box<dyn std::any::Any + Send + Sync>,
}

/// Trait implemented by the derive macro.
///
/// Access the generated model as `Self::MODEL`.
pub trait Entity {
    /// Statically generated model for the entity.
    const MODEL: &'static EntityModel;

    /// Return the entity model.
    fn entity_model(&self) -> &'static EntityModel {
        Self::MODEL
    }
}
