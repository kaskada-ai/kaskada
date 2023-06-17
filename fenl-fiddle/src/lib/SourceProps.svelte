<script lang="ts">
  import { Label, Input, Select } from "flowbite-svelte";

  export let sourceName = "purchases";
  export let timeColumnName = "";
  export let entityColumnName = "";
  export let columnNames: { value: string; name: string }[] = [];
</script>

<div class="grid gap-6 mb-6 md:grid-cols-3">
  <div>
    <Label>
      Source name:
      <Input
        type="text"
        class="mt-2"
        name="source-name"
        required
        bind:value={sourceName}
      />
    </Label>
  </div>
  <div>
    <Label>
      Entity column name:
      <Select class="mt-2" items={columnNames} bind:value={entityColumnName} />
    </Label>
  </div>
  <div>
    <Label>
      Time column name:
      <Select class="mt-2" items={columnNames} bind:value={timeColumnName} />
    </Label>
  </div>
</div>
