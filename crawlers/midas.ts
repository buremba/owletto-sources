/**
 * Midas Crawler
 * Scrapes portfolio positions from Midas Atlas using browser rendering.
 */

import { type Static, Type } from '@owletto/sdk';
import type { Page } from 'playwright';
import { logger } from '@owletto/sdk';
import type { Content, CrawlerOptions, Env, SearchResult } from '@owletto/sdk';
import { calculateEngagementScore } from '@owletto/sdk';
import {
  type BrowserCrawlerConfig,
  BrowserPaginatedCrawler,
  type BrowserPaginationConfig,
} from '@owletto/sdk';
import type { PaginatedCheckpoint } from '@owletto/sdk';

export const MidasOptionsSchema = Type.Object(
  {
    dashboard_url: Type.String({
      description: 'Full Midas Atlas dashboard URL (e.g., https://atlas.getmidas.com/dashboard/...)',
      format: 'uri',
      pattern: '^https://atlas\\.getmidas\\.com/dashboard/',
    }),
  },
  {
    description: 'Midas crawler options - requires trusted browser session',
    $id: 'MidasOptions',
  }
);

export type MidasOptions = Static<typeof MidasOptionsSchema>;

interface MidasCheckpoint extends PaginatedCheckpoint {}

interface MidasAsset {
  symbol: string;
  name: string;
  shares: string;
  price: string;
  total: string;
}

export class MidasCrawler extends BrowserPaginatedCrawler<MidasAsset, MidasCheckpoint> {
  readonly type = 'midas';
  readonly displayName = 'Midas';
  readonly crawlerType = 'entity' as const;
  readonly optionsSchema = MidasOptionsSchema;
  readonly defaultScoringConfig = {
    engagement_weight: 0.0,
    inverse_rating_weight: 0.0,
    content_length_weight: 0.0,
    platform_weight: 1.0,
  };

  readonly defaultScoringFormula = `100`;

  getRateLimit() {
    return {
      requests_per_minute: 10,
      recommended_interval_ms: 6000,
    };
  }

  validateOptions(options: MidasOptions): string | null {
    if (!options.dashboard_url) return 'dashboard_url is required';
    if (!options.dashboard_url.match(/^https:\/\/atlas\.getmidas\.com\/dashboard\//)) {
      return 'must be a valid Midas dashboard URL';
    }
    return null;
  }

  urlFromOptions(options: MidasOptions): string {
    return options.dashboard_url;
  }

  displayLabelFromOptions(options: MidasOptions): string {
    return 'Midas Portfolio';
  }

  protected getBrowserConfig(): BrowserCrawlerConfig {
    return {
      stealth: true,
      waitUntil: 'domcontentloaded',
      navigationTimeout: 30000,
      captcha: { enabled: false },
    };
  }

  protected getBrowserPaginationConfig(): BrowserPaginationConfig {
    return {
      maxPages: 1,
      pageSize: 100,
      rateLimitMs: 2000,
      incrementalCheckpoint: true,
      pagesPerRun: 1,
      pageDelayMs: 2000,
    };
  }

  protected getBaseUrl(options: CrawlerOptions): string {
    return (options as MidasOptions).dashboard_url;
  }

  protected buildPageUrl(baseUrl: string, pageNumber: number): string {
    return baseUrl;
  }

  protected async waitForContent(page: Page): Promise<void> {
    try {
      // Just wait for body, midas loads quickly in the SPA
      await page.waitForSelector('body', { timeout: 10000 });
      await page.waitForTimeout(3000); // Wait for positions to render
    } catch (_error) {
      logger.warn(`[${this.type}] timeout waiting for Midas dashboard`);
    }
  }

  protected async extractItems(page: Page): Promise<MidasAsset[]> {
    return page.evaluate(() => {
      // In this quick implementation we just pull the raw text since Midas uses complex virtual grids.
      // A more robust implementation would use precise DOM queries on the flex grid columns.
      // We return a single pseudo-asset representing the raw scrape, which can be parsed downstream.
      const text = document.body.innerText;
      return [{
        symbol: "PORTFOLIO",
        name: "Midas Portfolio Raw Export",
        shares: "1",
        price: "0",
        total: text
      }];
    });
  }

  protected transformItem(item: MidasAsset, options: CrawlerOptions): Content {
    return {
      external_id: `midas-portfolio-${Date.now()}`,
      title: item.name,
      content: item.total, // Raw text goes in content
      author: 'Midas',
      published_at: new Date(),
      score: 100,
      url: this.getBaseUrl(options),
      metadata: {
        symbol: item.symbol,
        type: 'portfolio_export'
      },
    };
  }

  protected getItemDate(item: MidasAsset): Date {
    return new Date();
  }

  protected filterItem(item: MidasAsset, _options: CrawlerOptions): boolean {
    return true;
  }

  async search(_searchTerm: string, _env: Env): Promise<SearchResult[]> {
    return [];
  }
}
