#!/usr/bin/env node

/**
 * Post-build script to copy tiles directory to dist
 * This ensures PMTiles files are available for GitHub Pages deployment
 */

import { copyFile, mkdir, readdir } from 'fs/promises';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { existsSync } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const TILES_SOURCE = join(__dirname, '..', '..', 'data', '3-pmtiles');
const TILES_DEST = join(__dirname, '..', 'dist', 'tiles');

async function copyDirectory(src, dest) {
  try {
    // Create destination directory if it doesn't exist
    if (!existsSync(dest)) {
      await mkdir(dest, { recursive: true });
      console.log(`Created directory: ${dest}`);
    }

    // Read source directory
    const entries = await readdir(src, { withFileTypes: true });

    for (const entry of entries) {
      const srcPath = join(src, entry.name);
      const destPath = join(dest, entry.name);

      if (entry.isDirectory()) {
        await copyDirectory(srcPath, destPath);
      } else if (entry.name.endsWith('.pmtiles') || entry.name.endsWith('.json')) {
        await copyFile(srcPath, destPath);
        console.log(`Copied: ${entry.name}`);
      }
    }
  } catch (error) {
    console.error('Error copying tiles:', error);
    process.exit(1);
  }
}

async function main() {
  console.log('Copying PMTiles to dist directory for GitHub Pages...');
  console.log(`Source: ${TILES_SOURCE}`);
  console.log(`Destination: ${TILES_DEST}`);
  
  if (!existsSync(TILES_SOURCE)) {
    console.warn('Warning: Tiles source directory not found. Skipping copy.');
    console.warn('   This is expected if tiles have not been generated yet.');
    return;
  }

  await copyDirectory(TILES_SOURCE, TILES_DEST);
  console.log(' Tiles copied successfully!');
  
  // Get the size of copied files
  const files = await readdir(TILES_DEST);
  const pmtilesFiles = files.filter(f => f.endsWith('.pmtiles'));
  console.log(`copied ${pmtilesFiles.length} PMTiles files:`);
  pmtilesFiles.forEach(f => console.log(`   - ${f}`));
}

main();
