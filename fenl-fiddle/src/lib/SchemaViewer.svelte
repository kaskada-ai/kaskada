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

  export let schemaFields: {
    name: string;
    data_type: { kind: { Primitive: number } };
  }[] = [];

  const decodeType = function (field: {
    name: string;
    data_type: { kind: { Primitive: number } };
  }): string {
    switch (field.data_type.kind.Primitive) {
      case 0:
        return "unspecified";
      case 1:
        return "null";
      case 2:
        return "bool";
      case 3:
        return "i8";
      case 4:
        return "i16";
      case 5:
        return "i32";
      case 6:
        return "i64";
      case 7:
        return "u8";
      case 8:
        return "u16";
      case 9:
        return "u32";
      case 10:
        return "u64";
      case 11:
        return "f16";
      case 12:
        return "f32";
      case 13:
        return "f64";
      case 14:
        return "string";
      case 15:
        return "interval_days";
      case 16:
        return "interval_months";
      case 17:
        return "duration_s";
      case 18:
        return "duration_ms";
      case 19:
        return "duration_us";
      case 20:
        return "duration_ns";
      case 21:
        return "timestamp_s";
      case 22:
        return "timestamp_ms";
      case 23:
        return "timestamp_us";
      case 24:
        return "timestamp_ns";
      case 25:
        return "json";
      case 26:
        return "date_32";
      default:
        return "unknown";
    }
  };
</script>

<Label>
  Schema:
  <div class="overscroll-none overflow-auto h-60">
    <Table class="mt-2">
      <TableHead
        class="text-xs uppercase text-gray-700 dark:text-gray-400 bg-gray-50 dark:bg-gray-700"
      >
        <TableHeadCell class="rounded-tl-lg">Index</TableHeadCell>
        <TableHeadCell>Field</TableHeadCell>
        <TableHeadCell class="rounded-tr-lg">Type</TableHeadCell>
      </TableHead>
      <TableBody tableBodyClass="divide-y">
        {#each schemaFields as field, index}
          <TableBodyRow>
            <TableBodyCell class="px-4 py-1 whitespace-nowrap font-medium"
              >{index}</TableBodyCell
            >
            <TableBodyCell class="px-4 py-1 whitespace-nowrap font-medium"
              >{field.name}</TableBodyCell
            >
            <TableBodyCell class="px-4 py-1 whitespace-nowrap font-medium"
              >{decodeType(field)}</TableBodyCell
            >
          </TableBodyRow>
        {/each}
      </TableBody>
    </Table>
  </div>
</Label>
