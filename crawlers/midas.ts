/**
 * Midas Crawler
 * Scrapes portfolio positions from Midas Atlas using browser rendering.
 */

import { type Static, Type } from '@owletto/sdk';
import type { Page } from 'playwright';
import { logger } from '@owletto/sdk';
import type { Content, CrawlerOptions, Env, SearchResult } from '@owletto/sdk';
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
  shares: number;
  price: string;
  value: string;
}

interface MidasPortfolio {
  total_usd: string;
  total_try: string;
  holdings: MidasAsset[];
  raw_text?: string;
}

export class MidasCrawler extends BrowserPaginatedCrawler<MidasPortfolio, MidasCheckpoint> {
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
      pageSize: 1,
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
      await page.waitForSelector('body', { timeout: 10000 });
      await page.waitForTimeout(3000); 
    } catch (_error) {
      logger.warn(`[${this.type}] timeout waiting for Midas dashboard`);
    }
  }

  protected async extractItems(page: Page): Promise<MidasPortfolio[]> {
    return page.evaluate(() => {
      const text = document.body.innerText;
      const lines = text.split('\\n').map(l => l.trim()).filter(Boolean);
      
      const holdings: any[] = [];
      
      // We will parse the text dynamically based on known markers
      // Look for "ABD Hisseleri"
      const usStartIdx = lines.indexOf("ABD Hisseleri");
      const trStartIdx = lines.indexOf("BIST Hisseleri");
      
      let usTickers = [];
      let trTickers = [];
      
      if (usStartIdx !== -1 && trStartIdx !== -1) {
        usTickers = lines.slice(usStartIdx + 1, trStartIdx);
      }
      
      let nextIdx = trStartIdx !== -1 ? trStartIdx + 1 : -1;
      while (nextIdx < lines.length && isNaN(parseInt(lines[nextIdx]))) {
        trTickers.push(lines[nextIdx]);
        nextIdx++;
      }
      
      // Parse US holdings values
      let currentIdx = nextIdx;
      let totalUsd = "0";
      let totalTry = "0";
      
      if (currentIdx < lines.length) {
        // Skip US totals
        const numUs = parseInt(lines[currentIdx]); // e.g. 17
        currentIdx++;
        totalUsd = lines[currentIdx]; // e.g. $991.792,83
        currentIdx += 3; // skip daily/total return
        
        for (let i = 0; i < usTickers.length; i++) {
          holdings.push({
            symbol: usTickers[i],
            shares: parseFloat(lines[currentIdx]?.replace(',', '.') || '0'),
            price: lines[currentIdx+1],
            value: lines[currentIdx+3],
            type: 'US'
          });
          currentIdx += 7; // 7 fields per row
        }
      }
      
      // Parse TR holdings values
      if (currentIdx < lines.length) {
        // Skip TR totals
        const numTr = parseInt(lines[currentIdx]); // e.g. 5
        currentIdx++;
        totalTry = lines[currentIdx];
        currentIdx += 3;
        
        for (let i = 0; i < trTickers.length; i++) {
          holdings.push({
            symbol: trTickers[i],
            shares: parseFloat(lines[currentIdx]?.replace('.', '').replace(',', '.') || '0'),
            price: lines[currentIdx+1],
            value: lines[currentIdx+3],
            type: 'TR'
          });
          currentIdx += 7;
        }
      }

      return [{
        total_usd: totalUsd,
        total_try: totalTry,
        holdings: holdings.length > 0 ? holdings : [],
        raw_text: text
      }];
    });
  }

  protected transformItem(item: MidasPortfolio, options: CrawlerOptions): Content {
    return {
      external_id: `midas-portfolio-${Date.now()}`,
      title: 'Midas Portfolio',
      content: '', // Left empty to be rendered dynamically by Lobu View Template
      author: 'Midas',
      published_at: new Date(),
      score: 100,
      url: this.getBaseUrl(options),
      metadata: {
        total_usd: item.total_usd,
        total_try: item.total_try,
        holdings: item.holdings,
        type: 'portfolio_export'
      },
    };
  }

  protected getItemDate(item: MidasPortfolio): Date {
    return new Date();
  }

  protected filterItem(item: MidasPortfolio, _options: CrawlerOptions): boolean {
    return true;
  }

  async search(_searchTerm: string, _env: Env): Promise<SearchResult[]> {
    return [];
  }
}
