<script lang="ts">
  import { Accordion, AccordionItem, Button } from "flowbite-svelte";
  import DarkMode from "./DarkMode.svelte";

  import DataSources from "./Icons/DataSources.svelte";
  import Settings from "./Icons/Settings.svelte";
</script>

<div class="flex-none w-60 p-2 h-full">
  <div class="flex items-center pl-2.5 mb-5">
    <img src="/kaskadaLogo.svg" class="h-6 mr-3 sm:h-7" alt="Kaskada" />
    <span
      class="self-center text-xl font-semibold whitespace-nowrap dark:text-white"
      >Fenl Fiddle</span
    >
  </div>

  <div class="flex flex-col h-full min-h-full">
    <Accordion flush class="flex-auto">
      <AccordionItem open>
        <span slot="header" class="text-base flex gap-2">
          <DataSources />
          <span>Data Sources</span>
        </span>
        <ul class="pl-4">
          <li>
            <Button
              color="alternative"
              class="bg-gray-300 text-black hover:text-black hover:bg-gray-300 dark:bg-gray-700 dark:text-white border-0 w-40"
              >Purchase</Button
            >
          </li>
          <li>
            <Button
              color="alternative"
              class="border-0 w-40 hover:text-black dark:bg-gray-900 dark:hover:bg-gray-800 dark:text-white"
              >Vendor</Button
            >
          </li>
          <li>
            <Button
              color="alternative"
              class="w-40 hover:text-black dark:hover:bg-gray-800 dark:text-white"
              >+ Add Another</Button
            >
          </li>
        </ul>
      </AccordionItem>
    </Accordion>

    <Accordion flush class="flex-none h-40">
      <AccordionItem open>
        <span slot="header" class="text-base flex gap-2">
          <Settings />
          <span>Settings</span>
        </span>
        <DarkMode />
      </AccordionItem>
    </Accordion>
  </div>
</div>
