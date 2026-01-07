# Memory Leak Detection avec Valgrind

Ce projet inclut plusieurs scripts pour détecter les memory leaks avec Valgrind.

## Scripts disponibles

### 1. `test_leaks.sh` - Check rapide des principales suites (RECOMMANDÉ)

Vérifie les 3 suites principales sans I/O (rapide, ~2 minutes).

**Usage:**
```bash
# D'abord build les tests
cargo test --no-run

# Puis lance le check
./test_leaks.sh
```

**Output:**
```
=== Memory Leak Check ===

1. basic_runtime
✓ OK

2. task_ordering
✓ OK

3. memory_leak_test
✓ OK
```

### 2. `valgrind_quick.sh` - Test détaillé d'une suite spécifique

Test avec output complet de Valgrind pour une suite donnée.

**Usage:**
```bash
./valgrind_quick.sh [test_name]
```

**Exemples:**
```bash
./valgrind_quick.sh memory_leak_test
./valgrind_quick.sh socket_operations
```

### 3. Commande manuelle Valgrind

Pour une analyse complète manuelle:

```bash
# Build les tests
cargo test --no-run

# Trouver le binaire
find target/debug/deps -name 'memory_leak_test-*' -type f -executable

# Lancer Valgrind
valgrind \
  --leak-check=full \
  --show-leak-kinds=all \
  --track-origins=yes \
  target/debug/deps/memory_leak_test-<hash>
```

## Résultats attendus

### Memory leaks corrigés ✅

Tous les tests **sauf** `test_task_panic` devraient montrer:
```
LEAK SUMMARY:
   definitely lost: 0 bytes in 0 blocks
```

### Leaks acceptables

#### `test_task_panic` - 40 bytes

Ce test intentionnellement panic une task. Quand une task panic, la mémoire ne peut pas être nettoyée proprement. C'est un comportement attendu et acceptable.

```rust
#[test]
#[should_panic]
fn test_task_panic() {
    // Cette task panic intentionnellement
    // Leak de ~40 bytes attendu
}
```

#### Leaks de la stdlib (possibly lost / still reachable)

Ces leaks sont normaux et viennent de la stdlib Rust:
- `possibly lost: 48 bytes` - Thread locals de la stdlib
- `still reachable: 544 bytes` - Structures internes de la stdlib

## Fix appliqué

Le leak principal (24,000 bytes dans 500 tasks) a été corrigé dans `src/task/handle.rs`:

```rust
// AVANT (leak):
if ret.is_ready() {
    self.task = None;  // Task jamais free !
}

// APRÈS (fixed):
if ret.is_ready() {
    if let Some(task) = self.task.take() {
        task.free();  // Free la task proprement
    }
}
```

## Suites de tests

1. **basic_runtime** - Tests basiques du runtime
2. **stress_tests** - Tests de stress (contient test_task_panic)
3. **task_ordering** - Tests d'ordre d'exécution
4. **memory_leak_test** - Tests intensifs d'allocations
5. **socket_operations** - Tests I/O basiques
6. **io_stress_tests** - Tests I/O de stress (lents avec Valgrind)

## Notes

- Les tests I/O (`io_stress_tests`, `socket_operations`) peuvent prendre du temps avec Valgrind
- Utiliser `--test-threads=1` pour éviter les race conditions dans Valgrind
- Les "possibly lost" et "still reachable" ne sont pas des vrais leaks
