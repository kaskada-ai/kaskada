<script lang="ts">
  import { invoke } from "@tauri-apps/api/tauri";
  import DataDrop from "./DataDrop.svelte";
  import SchemaViewer from "./SchemaViewer.svelte";
  import Terminal from "./Terminal.svelte";

  let expression = "";
  let tableName: string;
  let timeColumnName: string;
  let entityColumnName: string;
  let csv: string;

  let schema = {
    fields: [],
  };

  let diagnostics: string[] = [];

  let computeResult = "";

  async function compute() {
    computeResult = await invoke("compute", {
      csv,
      tableName,
      entityColumnName,
      timeColumnName,
    });
  }

  async function compile() {
    // Learn more about Tauri commands at https://tauri.app/v1/guides/features/command
    let compileResult: {
      fenl_diagnostics: {
        fenl_diagnostics: [
          {
            formatted: string;
          }
        ];
      };
      result_type: {
        kind: {
          Struct: {
            fields: string[];
          };
        };
      };
    } = await invoke("compile", {
      expression,
      tableName,
      entityColumnName,
      timeColumnName,
    });

    diagnostics = [];
    if ("fenl_diagnostics" in compileResult) {
      if ("fenl_diagnostics" in compileResult.fenl_diagnostics) {
        for (let element of compileResult.fenl_diagnostics.fenl_diagnostics) {
          diagnostics.push(element.formatted);
        }
      }
    }

    if ("result_type" in compileResult) {
      if (
        "kind" in compileResult.result_type &&
        compileResult.result_type.kind != null
      ) {
        if ("Struct" in compileResult.result_type.kind) {
          schema = compileResult.result_type.kind.Struct;
        }
      }
    }
  }
</script>

<div class="row">
  <DataDrop bind:tableName bind:timeColumnName bind:entityColumnName bind:csv />
</div>
<div>
  <form class="row" on:submit|preventDefault={compute}>
    <textarea
      on:keyup={compile}
      on:change={compile}
      id="data-input"
      placeholder="fenl..."
      bind:value={expression}
      cols="80"
      rows="10"
    />
    <button type="submit">Compute</button>
    <Terminal lines={diagnostics} />
  </form>
  <p>
    <SchemaViewer {schema} />
  </p>
  <section>
    {computeResult}
  </section>
</div>
