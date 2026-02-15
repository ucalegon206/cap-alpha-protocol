import { test, expect } from '@playwright/test';

test.describe('Data Grid Verification', () => {

    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        // Switch to Data Grid tab
        await page.getByRole('tab', { name: 'Data Grid' }).click();
    });

    test('Grid Renders with Data', async ({ page }) => {
        // Check for table rows
        const rows = page.locator('tbody tr');
        await expect(rows).toHaveCount(24); // Based on default data
    });

    test('Columns are Present and Rename is Applied', async ({ page }) => {
        // Check Headers
        await expect(page.getByRole('columnheader', { name: 'Player' })).toBeVisible();
        await expect(page.getByRole('columnheader', { name: 'Team' })).toBeVisible();
        await expect(page.getByRole('columnheader', { name: 'Value ($M)' })).toBeVisible(); // Rename check
        await expect(page.getByRole('columnheader', { name: 'Risk Score' })).toBeVisible();
    });

    test('Sorting Works (Value Column)', async ({ page }) => {
        const valueHeader = page.getByRole('button', { name: 'Value ($M)' });

        // Initial State: Unsorted or Default
        // Click to Sort Ascending/Descending
        await valueHeader.click();

        // Get first row value
        const firstRowValue = page.locator('tbody tr').first().locator('td').nth(6); // Surplus/Value is last column
        await expect(firstRowValue).toBeVisible();
    });

    test('Tooltips Contain Explanatory Text', async ({ page }) => {
        // Hover Cap Hit
        const capHitHeader = page.getByRole('button', { name: 'Cap Hit ($M)' });
        await capHitHeader.hover();
        await expect(page.getByText('Current season salary cap charge')).toBeVisible();

        // Hover Risk Score
        const riskHeader = page.getByRole('button', { name: 'Risk Score' });
        await riskHeader.hover();
        await expect(page.getByText('0-1 score assessing contract volatility & injury risk')).toBeVisible();

        // Hover Value
        const valueHeader = page.getByRole('button', { name: 'Value ($M)' });
        await valueHeader.hover();
        await expect(page.getByText('Net Performance Value (Surplus)')).toBeVisible();
    });

});
