<script lang="ts">
  import { invoke } from "@tauri-apps/api/tauri";
  import SchemaViewer from "./SchemaViewer.svelte";

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

  let schema = {
    fields: [],
  };
  export let tableName = "purchases";
  export let timeColumnName = "";
  export let entityColumnName = "";
  let columnNames = [];

  async function get_schema() {
    // Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
    schema = await invoke("get_schema", { csv });

    if ("fields" in schema) {
      columnNames = [];
      entityColumnName = "";
      timeColumnName = "";
      for (let element of schema.fields) {
        columnNames.push(element.name);
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

<div>
  <form class="row" on:submit|preventDefault={get_schema}>
    <textarea
      id="data-input"
      placeholder="Copy and Paste CSV..."
      bind:value={csv}
      cols="80"
      rows="10"
    />
    <button type="submit">Load Data</button>
  </form>

  <form>
    <p>
      <label>
        Table Name:
        <input name="table-name" type="text" bind:value={tableName} />
      </label>
    </p>
    <p>
      <label>
        Entity Column Name:
        <select name="entity-column-name" bind:value={entityColumnName}>
          {#each columnNames as columnName}
            <option value={columnName}>
              {columnName}
            </option>
          {/each}
        </select>
      </label>
    </p>
    <p>
      <label>
        Time Column Name:
        <select name="time-column-name" bind:value={timeColumnName}>
          {#each columnNames as columnName}
            <option value={columnName}>
              {columnName}
            </option>
          {/each}
        </select>
      </label>
    </p>
  </form>
  <p>
    <SchemaViewer {schema} />
  </p>
</div>
