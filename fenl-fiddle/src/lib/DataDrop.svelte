<script lang="ts">
  import { invoke } from "@tauri-apps/api/tauri";
  import SchemaViewer from "./SchemaViewer.svelte";
  import SourceProps from "./SourceProps.svelte";
  import { Textarea, Label } from "flowbite-svelte";

  export let csv = `id,purchase_time,customer_id,vendor_id,amount,subsort_id
cb_001,2020-01-01T00:00:00.000000000+00:00,karen,chum_bucket,9,0
kk_001,2020-01-01T00:00:00.000000000+00:00,patrick,krusty_krab,3,1
cb_002,2020-01-02T00:00:00.000000000+00:00,karen,chum_bucket,2,2
kk_002,2020-01-02T00:00:00.000000000+00:00,patrick,krusty_krab,5,3
cb_003,2020-01-03T00:00:00.000000000+00:00,karen,chum_bucket,4,4
kk_003,2020-01-03T00:00:00.000000000+00:00,patrick,krusty_krab,12,5
cb_004,2020-01-04T00:00:00.000000000+00:00,patrick,chum_bucket,5000,6
cb_005,2020-01-04T00:00:00.000000000+00:00,karen,chum_bucket,3,7
cb_006,2020-01-05T00:00:00.000000000+00:00,karen,chum_bucket,5,8
kk_004,2020-01-05T00:00:00.000000000+00:00,patrick,krusty_krab,9,9
`;

  let schema: {
    fields: {
      name: string;
      data_type: { kind: { Primitive: number } };
    }[];
  } = { fields: [] };

  export let sourceName = "purchases";
  export let timeColumnName = "";
  export let entityColumnName = "";
  let columnNames: { value: string; name: string }[] = [];

  async function get_schema() {
    // Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
    schema = await invoke("get_schema", { csv });

    if ("fields" in schema) {
      columnNames = [];
      entityColumnName = "";
      timeColumnName = "";
      for (let element of schema.fields) {
        columnNames.push({ value: element.name, name: element.name });
        if (
          entityColumnName == "" &&
          element.name.toLowerCase().includes("id")
        ) {
          entityColumnName = element.name;
        }
        if (
          timeColumnName == "" &&
          element.name.toLowerCase().includes("time")
        ) {
          timeColumnName = element.name;
        }
      }
    }
  }
</script>

<div class="flex flex-row gap-4">
  <div class="basis-3/4">
    <Label
      >Data:
      <Textarea
        on:blur={get_schema}
        id="data-input"
        rows="10"
        class="mt-2"
        placeholder="Copy and Paste CSV..."
        bind:value={csv}
      />
    </Label>

    <SourceProps
      bind:sourceName
      bind:entityColumnName
      bind:timeColumnName
      bind:columnNames
    />
  </div>
  <div class="basis-1/4">
    <SchemaViewer schemaFields={schema.fields} />
  </div>
</div>
