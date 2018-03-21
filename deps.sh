nix-shell -p "haskellPackages.ghcWithPackages (pkgs: with pkgs; [ base clock stm ])"
