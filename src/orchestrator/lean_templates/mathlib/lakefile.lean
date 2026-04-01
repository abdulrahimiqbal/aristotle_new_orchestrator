import Lake
open Lake DSL

-- Align toolchain with https://github.com/leanprover-community/mathlib4/wiki/Using-mathlib4-as-a-dependency
require mathlib from git
  "https://github.com/leanprover-community/mathlib4"

package «orch_workspace» where

@[default_target]
lean_lib OrchWorkspace where
