use frugalos::Error;

macro_rules! async {
    ($e:expr) => {{
        use futures::Future;
        let future = $e.map_err(::Error::from);
        Box::new(future)
    }};
}

mod frugalos;

pub use self::frugalos::{FrugalosClient, FrugalosClientRegistry};

pub type AsyncResult<T> = Box<dyn futures::Future<Item = T, Error = Error> + Send + 'static>;
