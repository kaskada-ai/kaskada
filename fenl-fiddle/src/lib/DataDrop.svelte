<script lang="ts">
  import { invoke } from "@tauri-apps/api/tauri";
  import SchemaViewer from "./SchemaViewer.svelte";
  import SourceProps from "./SourceProps.svelte";
  import { Label, TabItem, Tabs, Textarea } from "flowbite-svelte";
  import CsvViewer from "./CsvViewer.svelte";
  import { prevent_default } from "svelte/internal";

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
    <Tabs
      contentClass="p-1 bg-gray-50 rounded-lg dark:bg-gray-800"
      activeClasses="px-4 py-2 text-primary-600 border-b-2 border-primary-600 dark:text-primary-500 dark:border-primary-500"
      inactiveClasses="px-4 py-2 border-b-2 border-transparent hover:text-gray-600 hover:border-gray-300 dark:hover:text-gray-300 text-gray-500 dark:text-gray-400"
    >
      <TabItem title="Data:" disabled />
      <TabItem title="Raw" open>
        <Textarea
          on:blur={get_schema}
          id="data-input"
          placeholder="Copy and Paste CSV..."
          bind:value={csv}
          class="overscroll-none overflow-auto h-48"
        />
      </TabItem>
      <TabItem title="Table">
        <div class="overscroll-none overflow-auto h-48">
          <CsvViewer csvData={csv} />
        </div>
      </TabItem>
    </Tabs>

    <SourceProps
      bind:sourceName
      bind:entityColumnName
      bind:timeColumnName
      bind:columnNames
    />
  </div>
  <div class="basis-1/4 mt-4">
    <SchemaViewer schemaFields={schema.fields} />
  </div>
</div>
