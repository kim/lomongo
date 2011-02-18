# Lomongo

_lol_ ...

Lomongo is a driver for MongoDB written in Scala and trying to be really
functional (not through using funny characters, though). It uses Netty for IO.

## Installation and Usage

Install using `sbt`:

    sbt update
    sbt install

For an example of how to use the driver, see `main.scala`.

## Motivation

Existing drivers for MongoDB are quite clumsy to work with from Scala, mainly
because they try to create a Scala interface on top of the Java driver, which is
not only very OO, but also very weakly typed. Thus, those drivers make heavy
use of implicit conversions, which predictably goes horribly wrong in
non-trivial use-cases. However, trying to create a functional, strongly typed
Scala driver requires one to re-implement the protocol and serialization layers,
hence this project may or may not reach production quality (and will be maintained
actively enough to stay there). Thus, patches and contributions are highly
welcome.

## Contributing

Send questions, bugs or patches to kim.altintop@gmail.com

## License

BSD-like, see LICENSE
