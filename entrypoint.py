import argparse

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from TaxPolicyCrawlerScrapy.spiders.chinatax.TaxPolicyCrawler import TaxPolicyCrawler
from TaxPolicyCrawlerScrapy.spiders.chinatax.TaxPolicyExplainCrawler import TaxPolicyExplainCrawler


SPIDER_REGISTRY = {
    TaxPolicyCrawler.name: TaxPolicyCrawler,
    TaxPolicyExplainCrawler.name: TaxPolicyExplainCrawler,
}


def parse_args():
    parser = argparse.ArgumentParser(description='Run tax policy crawlers.')
    parser.add_argument(
        '--spiders',
        default='TaxPolicyCrawler,TaxPolicyExplainCrawler',
        help='Comma-separated spider names.',
    )
    parser.add_argument('--max-pages', type=int, help='Override CHINATAX_MAX_PAGES.')
    parser.add_argument('--page-size', type=int, help='Override CHINATAX_PAGE_SIZE.')
    parser.add_argument('--export-excel', choices=['0', '1'], help='Override EXPORT_EXCEL.')
    parser.add_argument('--use-elasticsearch', choices=['0', '1'], help='Override USE_ELASTICSEARCH.')
    return parser.parse_args()


def build_settings(args):
    settings = get_project_settings()

    if args.max_pages is not None:
        settings.set('CHINATAX_MAX_PAGES', args.max_pages, priority='cmdline')
    if args.page_size is not None:
        settings.set('CHINATAX_PAGE_SIZE', args.page_size, priority='cmdline')
    if args.export_excel is not None:
        settings.set('EXPORT_EXCEL', bool(int(args.export_excel)), priority='cmdline')
    if args.use_elasticsearch is not None:
        settings.set('USE_ELASTICSEARCH', bool(int(args.use_elasticsearch)), priority='cmdline')

    item_pipelines = {}
    if settings.getbool('USE_ELASTICSEARCH'):
        item_pipelines['TaxPolicyCrawlerScrapy.pipelines.ElasticSearchPipeline.ElasticSearchPipeline'] = 400
    if settings.getbool('EXPORT_EXCEL'):
        item_pipelines['TaxPolicyCrawlerScrapy.pipelines.ExcelPipeline.ExcelPipeline'] = 450
    settings.set('ITEM_PIPELINES', item_pipelines, priority='cmdline')
    return settings


def parse_spiders(raw_value):
    spiders = []
    for name in [x.strip() for x in raw_value.split(',') if x.strip()]:
        if name not in SPIDER_REGISTRY:
            raise ValueError(
                'Unknown spider "{}". Available: {}'.format(
                    name, ', '.join(sorted(SPIDER_REGISTRY.keys()))
                )
            )
        spiders.append(SPIDER_REGISTRY[name])
    return spiders


def main():
    args = parse_args()
    spiders = parse_spiders(args.spiders)
    process = CrawlerProcess(build_settings(args))
    for spider in spiders:
        process.crawl(spider)
    process.start()


if __name__ == '__main__':
    main()
