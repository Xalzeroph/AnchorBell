# Legacy Python prototype

This directory contains the original research prototype. It is intentionally
excluded from the production build, runtime, and CI test path.

The canonical implementation is the Rust workspace under engine/. Changes
to production behavior must be made there and covered by the Rust gates.
The prototype is retained only for historical comparison and migration notes.
