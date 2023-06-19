<script lang="ts">
  import Compute from "./lib/Compute.svelte";
  import SideBar from "./lib/SideBar.svelte";
</script>

<div class="flex flex-row gap-2 pr-4">
  <div class="flex-none w-60">
    <SideBar />
  </div>
  <div class="grow">
    <Compute />
  </div>
</div>

<style>
</style>
