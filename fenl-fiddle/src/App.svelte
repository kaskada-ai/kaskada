<script lang="ts">
  import DataDrop from './lib/DataDrop.svelte';
</script>

<main class="container">
  <div class="row">
    <DataDrop />
  </div>
</main>

<style>
  .logo.vite:hover {
    filter: drop-shadow(0 0 2em #747bff);
  }

  .logo.svelte:hover {
    filter: drop-shadow(0 0 2em #ff3e00);
  }
</style>