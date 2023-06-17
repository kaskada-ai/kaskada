<script lang="ts">
  import Compute from "./lib/Compute.svelte";
  import { Alert } from "flowbite-svelte";
</script>

<div class="p-8">
  <Alert>
    <span class="font-medium">Info alert!</span> Change a few things up and try submitting
    again.
  </Alert>
</div>
<main class="container">
  <div class="row">
    <Compute />
  </div>
</main>

<style>
</style>
