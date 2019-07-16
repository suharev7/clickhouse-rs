extern crate cc;

fn main() {
    let mut compiler = cc::Build::new();
    compiler.file("src/cc/city.cc").cpp(true).opt_level(3);

    compiler.compile("libchcityhash.a");
}
