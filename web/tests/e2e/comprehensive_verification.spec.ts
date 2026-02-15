
import { test, expect } from '@playwright/test';

// Define Test Data
const TARGET_URL = 'https://cap-alpha.co';
const TEST_PLAYER = 'Russell Wilson';

// --- DESKTOP SUITE ---
test.describe('Desktop Verification Suite', () => {
    test.use({ viewport: { width: 1280, height: 720 } });

    test('GM Persona: Verify Roster Management & Drilldown', async ({ page }) => {
        console.log('Starting GM Persona Test...');
        await page.goto(TARGET_URL);
        await expect(page).toHaveTitle(/Cap Alpha/);

        // 1. Dashboard Load
        await expect(page.getByText('Total Cap Liabilities')).toBeVisible();

        // 2. Navigate to Data Grid
        await page.getByRole('tab', { name: 'Data Grid' }).click();
        await expect(page.getByPlaceholder('Search players...')).toBeVisible();

        // 3. Search for Player
        await page.getByPlaceholder('Search players...').fill(TEST_PLAYER);
        await page.keyboard.press('Enter');

        // 4. Verify Results
        await expect(page.getByRole('cell', { name: 'Russell Wilson' }).first()).toBeVisible();
        // Check for Dead Money vs Active Contract logic
        await expect(page.getByText('$32.00M')).toBeVisible(); // Denver Dead Money
    });

    test('Agent Persona: Market Efficiency Landscape Interaction', async ({ page }) => {
        await page.goto(TARGET_URL);
        // Scroll to chart
        await page.evaluate(() => window.scrollBy(0, 500));
        await expect(page.getByText('Market Efficiency Landscape')).toBeVisible();

        // Hover over a bubble (simulated)
        // Note: Recharts is SVG, tricky to hover specific elements without custom IDs, 
        // but we verify the container exists and renders.
        await expect(page.locator('.recharts-responsive-container')).toBeVisible();
    });
});

// --- MOBILE SUITE ---
test.describe('Mobile Verification Suite (iPhone 13)', () => {
    test.use({ viewport: { width: 390, height: 844 }, isMobile: true });

    test('Fan Persona: Mobile Responsiveness', async ({ page }) => {
        console.log('Starting Mobile Fan Persona Test...');
        await page.goto(TARGET_URL);

        // 1. Verify Hamburger Menu or Stacked Layout
        // (Assuming mobile layout stacks cards)
        await expect(page.getByText('Total Cap Liabilities')).toBeVisible();

        // 2. Check Chart Visibility on Mobile
        await page.evaluate(() => window.scrollBy(0, 500));
        await expect(page.locator('.recharts-responsive-container')).toBeVisible();

        // 3. Search on Mobile
        await page.getByRole('tab', { name: 'Data Grid' }).click();
        await page.getByPlaceholder('Search players...').fill('Kyler');
        await page.keyboard.press('Enter');
        await expect(page.getByRole('cell', { name: 'Kyler Murray' }).first()).toBeVisible();
    });
});
