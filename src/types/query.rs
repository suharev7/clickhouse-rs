#[derive(Clone, Debug)]
pub struct Query {
    sql: String,
    id: String,
}

impl Query {
    pub fn new(sql: &str) -> Self {
        Self {
            sql: sql.to_string(),
            id: "".to_string(),
        }
    }

    pub fn id(self, id: &str) -> Self {
        Self {
            id: id.to_string(),
            ..self
        }
    }

    pub(crate) fn get_sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn get_id(&self) -> &str {
        &self.id
    }

    pub(crate) fn map_sql<F>(self, f: F) -> Self
    where
        F: Fn(&str) -> String,
    {
        Self {
            sql: f(&self.sql),
            ..self
        }
    }
}

impl<'a> From<&'a str> for Query {
    fn from(source: &str) -> Self {
        Self::new(source)
    }
}
