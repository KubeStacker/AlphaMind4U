<template>
  <div class="space-y-3 md:space-y-4 max-w-6xl mx-auto pb-6">
    <SentimentPanel />
    <MainlinePanel ref="mainlinePanelRef" />
  </div>
</template>

<script setup>
import { ref } from 'vue';
import SentimentPanel from '@/components/dashboard/SentimentPanel.vue';
import MainlinePanel from '@/components/dashboard/MainlinePanel.vue';

const mainlinePanelRef = ref(null);
</script>

<style scoped>
</style>
