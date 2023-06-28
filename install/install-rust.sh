curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
rustup install 1.70.0
rustup default 1.70.0
rustup component add rustfmt
rustup default stable

