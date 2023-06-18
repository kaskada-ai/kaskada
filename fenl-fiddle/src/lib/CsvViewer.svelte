<script lang="ts">
  import {
    Label,
    Table,
    TableBody,
    TableBodyCell,
    TableBodyRow,
    TableHead,
    TableHeadCell,
  } from "flowbite-svelte";

  import * as d3 from "d3";

  export let csvData: string = "";
  export let title: string = "Results:";

  let columnNames: string[] = [];
  let rows: { string: any }[] = [];

  $: {
    rows = d3.csvParse(csvData);
    if (rows.length > 0) {
      columnNames = Object.keys(rows[0]);
    }
    console.log(rows);
  }
</script>

<Table striped={true}>
  <TableHead
    class="text-xs uppercase text-gray-700 dark:text-gray-400 bg-gray-50 dark:bg-gray-700"
  >
    <TableHeadCell>Index</TableHeadCell>
    {#each columnNames as columnName}
      <TableHeadCell>{columnName}</TableHeadCell>
    {/each}
  </TableHead>
  <TableBody tableBodyClass="divide-y">
    {#each rows as row, index}
      <TableBodyRow>
        <TableBodyCell class="px-4 py-1 whitespace-nowrap font-medium"
          >{index}</TableBodyCell
        >
        {#each columnNames as columnName}
          <TableBodyCell class="px-4 py-1 whitespace-nowrap font-medium"
            >{row[columnName]}</TableBodyCell
          >
        {/each}
      </TableBodyRow>
    {/each}
  </TableBody>
</Table>
