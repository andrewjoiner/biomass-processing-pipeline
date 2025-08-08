#!/usr/bin/env python3
"""
Production Test Suite - Comprehensive testing for nationwide biomass processing pipeline
Tests scalability, error handling, and performance at various scales
"""

import logging
import time
import traceback
from datetime import datetime
from typing import Dict, List, Tuple

from ..pipeline.comprehensive_biomass_processor_v1 import comprehensive_biomass_processor
from ..core.database_manager_v1 import database_manager
from ..core.blob_manager_v1 import blob_manager

logger = logging.getLogger(__name__)

class ProductionTestSuite:
    """
    Comprehensive test suite for production biomass processing pipeline
    Tests system stability and performance at increasing scales
    """
    
    def __init__(self):
        self.processor = comprehensive_biomass_processor
        self.db_manager = database_manager
        self.blob_manager = blob_manager
        
        # Test configuration
        self.test_scenarios = {
            'small_scale': {'parcels': 10, 'description': '10 parcels - basic functionality'},
            'medium_scale': {'parcels': 100, 'description': '100 parcels - batch processing'},
            'large_scale': {'parcels': 1000, 'description': '1000 parcels - memory management'},
            'xl_scale': {'parcels': 5000, 'description': '5000 parcels - connection pool stress'},
            'xxl_scale': {'parcels': 10000, 'description': '10000+ parcels - full county scale'}
        }
        
        # Known good test counties with reliable data
        self.test_counties = [
            ('49', '033', 'Rich County, Utah'),  # Successfully processed before
            ('49', '005', 'Cache County, Utah'),
            ('49', '057', 'Washington County, Utah')
        ]
    
    def run_full_test_suite(self) -> Dict:
        """
        Run comprehensive test suite at all scales
        
        Returns:
            Complete test results summary
        """
        logger.info("🧪 Starting Production Test Suite")
        logger.info("="*60)
        
        test_results = {
            'suite_start': datetime.now().isoformat(),
            'tests_executed': [],
            'tests_passed': 0,
            'tests_failed': 0,
            'performance_metrics': {},
            'error_summary': []
        }
        
        # Pre-test system checks
        system_check = self._run_system_checks()
        test_results['system_check'] = system_check
        
        if not system_check['all_systems_ready']:
            logger.error("❌ System checks failed - aborting test suite")
            return test_results
        
        # Run test scenarios in order of increasing scale
        for scenario_name, config in self.test_scenarios.items():
            logger.info(f"\n{'='*40}")
            logger.info(f"🔬 Test Scenario: {scenario_name}")
            logger.info(f"📊 {config['description']}")
            logger.info(f"{'='*40}")
            
            try:
                scenario_result = self._run_test_scenario(scenario_name, config)
                test_results['tests_executed'].append(scenario_result)
                
                if scenario_result['success']:
                    test_results['tests_passed'] += 1
                    logger.info(f"✅ {scenario_name} PASSED")
                else:
                    test_results['tests_failed'] += 1
                    logger.error(f"❌ {scenario_name} FAILED: {scenario_result.get('error')}")
                    # Continue with other tests even if one fails
                    
            except Exception as e:
                test_results['tests_failed'] += 1
                error_info = {
                    'scenario': scenario_name,
                    'error': str(e),
                    'traceback': traceback.format_exc()
                }
                test_results['error_summary'].append(error_info)
                logger.error(f"💥 {scenario_name} CRASHED: {e}")
        
        # Performance analysis
        test_results['performance_metrics'] = self._analyze_performance_metrics(test_results)
        test_results['suite_end'] = datetime.now().isoformat()
        
        # Final summary
        self._log_final_summary(test_results)
        
        return test_results
    
    def _run_system_checks(self) -> Dict:
        """Run pre-test system checks"""
        logger.info("🔍 Running system checks...")
        
        checks = {
            'database_connections': False,
            'blob_storage_access': False,
            'test_data_available': False,
            'all_systems_ready': False
        }
        
        try:
            # Test database connections
            db_status = self.db_manager.test_connections()
            checks['database_connections'] = all(db_status.values())
            logger.info(f"Database connections: {'✅' if checks['database_connections'] else '❌'}")
            
            # Test blob storage
            blob_stats = self.blob_manager.get_cache_stats()
            checks['blob_storage_access'] = True  # If we got stats, connection works
            logger.info(f"Blob storage access: {'✅' if checks['blob_storage_access'] else '❌'}")
            
            # Test data availability (check test counties exist)
            test_county = self.test_counties[0]
            parcels = self.db_manager.get_county_parcels(test_county[0], test_county[1], limit=5)
            checks['test_data_available'] = len(parcels) > 0
            logger.info(f"Test data available: {'✅' if checks['test_data_available'] else '❌'}")
            
            checks['all_systems_ready'] = all([
                checks['database_connections'],
                checks['blob_storage_access'], 
                checks['test_data_available']
            ])
            
        except Exception as e:
            logger.error(f"System check error: {e}")
            checks['error'] = str(e)
        
        return checks
    
    def _run_test_scenario(self, scenario_name: str, config: Dict) -> Dict:
        """Run a single test scenario"""
        target_parcels = config['parcels']
        start_time = time.time()
        
        # Select appropriate test county based on scale
        if target_parcels <= 100:
            test_county = self.test_counties[0]  # Smallest county
        elif target_parcels <= 1000:
            test_county = self.test_counties[1]  # Medium county
        else:
            test_county = self.test_counties[2]  # Larger county
        
        state_fips, county_fips, county_name = test_county
        
        logger.info(f"🎯 Target: {target_parcels} parcels from {county_name}")
        
        try:
            # Run processing with parcel limit
            result = self.processor.process_county_comprehensive(
                state_fips, county_fips,
                max_parcels=target_parcels,
                batch_size=min(500, target_parcels),  # Appropriate batch size
                enable_parallel=target_parcels > 50
            )
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            if result['success']:
                parcels_processed = result['processing_summary']['parcels_processed']
                processing_rate = parcels_processed / processing_time if processing_time > 0 else 0
                
                scenario_result = {
                    'scenario': scenario_name,
                    'success': True,
                    'target_parcels': target_parcels,
                    'actual_parcels': parcels_processed,
                    'processing_time_seconds': round(processing_time, 2),
                    'processing_rate_per_second': round(processing_rate, 3),
                    'processing_rate_per_hour': round(processing_rate * 3600, 0),
                    'test_county': county_name,
                    'error_rate': result['processing_summary']['processing_errors'] / max(parcels_processed, 1),
                    'memory_usage': self._get_memory_usage(),
                    'performance_metrics': result['processing_summary']
                }
                
                # Validate results quality
                quality_check = self._validate_result_quality(result)
                scenario_result['quality_check'] = quality_check
                
                logger.info(f"📊 Processed: {parcels_processed} parcels in {processing_time:.1f}s "
                           f"({processing_rate:.1f} parcels/sec)")
                
            else:
                scenario_result = {
                    'scenario': scenario_name,
                    'success': False,
                    'error': result.get('error', 'Unknown error'),
                    'processing_time_seconds': round(processing_time, 2)
                }
            
            return scenario_result
            
        except Exception as e:
            return {
                'scenario': scenario_name,
                'success': False,
                'error': str(e),
                'processing_time_seconds': round(time.time() - start_time, 2)
            }
    
    def _validate_result_quality(self, result: Dict) -> Dict:
        """Validate quality of processing results"""
        quality_metrics = {
            'has_biomass_data': False,
            'confidence_score_ok': False,
            'data_completeness_ok': False,
            'overall_quality': 'poor'
        }
        
        try:
            # Check if biomass totals are reasonable
            biomass_totals = result.get('biomass_totals', {})
            total_biomass = biomass_totals.get('total_biomass_tons', 0)
            quality_metrics['has_biomass_data'] = total_biomass > 0
            
            # Check confidence scores
            avg_confidence = result.get('data_quality', {}).get('average_confidence', 0)
            quality_metrics['confidence_score_ok'] = avg_confidence >= 0.5
            
            # Check forest/crop coverage rates (should have some productive land)
            forest_rate = result.get('data_quality', {}).get('forest_coverage_rate', 0)
            crop_rate = result.get('data_quality', {}).get('crop_coverage_rate', 0)
            quality_metrics['data_completeness_ok'] = (forest_rate + crop_rate) > 0.1
            
            # Overall quality assessment
            passed_checks = sum([
                quality_metrics['has_biomass_data'],
                quality_metrics['confidence_score_ok'],
                quality_metrics['data_completeness_ok']
            ])
            
            if passed_checks >= 3:
                quality_metrics['overall_quality'] = 'excellent'
            elif passed_checks >= 2:
                quality_metrics['overall_quality'] = 'good'
            elif passed_checks >= 1:
                quality_metrics['overall_quality'] = 'acceptable'
            
        except Exception as e:
            quality_metrics['validation_error'] = str(e)
        
        return quality_metrics
    
    def _get_memory_usage(self) -> Dict:
        """Get current memory usage statistics"""
        try:
            import psutil
            process = psutil.Process()
            return {
                'memory_mb': round(process.memory_info().rss / 1024 / 1024, 2),
                'memory_percent': round(process.memory_percent(), 2)
            }
        except ImportError:
            return {'memory_mb': 'psutil_not_available'}
        except Exception as e:
            return {'memory_error': str(e)}
    
    def _analyze_performance_metrics(self, test_results: Dict) -> Dict:
        """Analyze performance trends across test scenarios"""
        performance_analysis = {
            'scalability_trend': 'unknown',
            'peak_performance': {'rate': 0, 'scenario': None},
            'efficiency_trend': [],
            'error_rates': []
        }
        
        try:
            successful_tests = [t for t in test_results['tests_executed'] if t.get('success')]
            
            if len(successful_tests) >= 2:
                # Analyze performance scaling
                rates = []
                for test in successful_tests:
                    rate = test.get('processing_rate_per_second', 0)
                    rates.append(rate)
                    
                    # Track peak performance
                    if rate > performance_analysis['peak_performance']['rate']:
                        performance_analysis['peak_performance'] = {
                            'rate': rate,
                            'scenario': test['scenario'],
                            'parcels': test.get('actual_parcels', 0)
                        }
                    
                    # Track efficiency and error rates
                    performance_analysis['efficiency_trend'].append({
                        'scenario': test['scenario'],
                        'parcels': test.get('actual_parcels', 0),
                        'rate_per_second': rate,
                        'error_rate': test.get('error_rate', 0)
                    })
                
                # Determine scalability trend
                if len(rates) >= 3:
                    early_avg = sum(rates[:2]) / 2
                    later_avg = sum(rates[-2:]) / 2
                    
                    if later_avg > early_avg * 0.8:
                        performance_analysis['scalability_trend'] = 'good'
                    elif later_avg > early_avg * 0.6:
                        performance_analysis['scalability_trend'] = 'moderate'
                    else:
                        performance_analysis['scalability_trend'] = 'poor'
        
        except Exception as e:
            performance_analysis['analysis_error'] = str(e)
        
        return performance_analysis
    
    def _log_final_summary(self, test_results: Dict):
        """Log comprehensive test suite summary"""
        logger.info("\n" + "="*80)
        logger.info("🏁 PRODUCTION TEST SUITE - FINAL SUMMARY")
        logger.info("="*80)
        
        # Test execution summary
        total_tests = test_results['tests_passed'] + test_results['tests_failed']
        success_rate = (test_results['tests_passed'] / total_tests * 100) if total_tests > 0 else 0
        
        logger.info(f"📊 Test Execution Summary:")
        logger.info(f"   Total scenarios: {total_tests}")
        logger.info(f"   ✅ Passed: {test_results['tests_passed']}")
        logger.info(f"   ❌ Failed: {test_results['tests_failed']}")
        logger.info(f"   📈 Success rate: {success_rate:.1f}%")
        
        # Performance summary
        perf = test_results.get('performance_metrics', {})
        peak = perf.get('peak_performance', {})
        
        if peak.get('rate', 0) > 0:
            logger.info(f"\n🚀 Performance Summary:")
            logger.info(f"   Peak rate: {peak['rate']:.1f} parcels/sec")
            logger.info(f"   Peak scenario: {peak['scenario']}")
            logger.info(f"   Peak parcels: {peak.get('parcels', 0):,}")
            logger.info(f"   Scalability: {perf.get('scalability_trend', 'unknown')}")
        
        # Quality assessment
        successful_tests = [t for t in test_results['tests_executed'] if t.get('success')]
        if successful_tests:
            quality_scores = [t.get('quality_check', {}).get('overall_quality', 'unknown') 
                            for t in successful_tests]
            excellent_count = quality_scores.count('excellent')
            good_count = quality_scores.count('good')
            
            logger.info(f"\n✨ Quality Summary:")
            logger.info(f"   Excellent quality: {excellent_count}/{len(successful_tests)}")
            logger.info(f"   Good quality: {good_count}/{len(successful_tests)}")
        
        # Readiness assessment
        logger.info(f"\n🎯 Production Readiness Assessment:")
        if success_rate >= 80 and peak.get('rate', 0) >= 1.0:
            logger.info("   ✅ PRODUCTION READY - System stable at scale")
        elif success_rate >= 60:
            logger.info("   ⚠️  NEEDS OPTIMIZATION - System functional but needs tuning")
        else:
            logger.info("   ❌ NOT READY - Critical issues need resolution")
        
        logger.info("="*80)
    
    def run_single_scale_test(self, target_parcels: int, test_county_index: int = 0) -> Dict:
        """Run a single test at specified scale"""
        config = {'parcels': target_parcels, 'description': f'{target_parcels} parcels - custom test'}
        return self._run_test_scenario(f'custom_{target_parcels}', config)
    
    def run_stress_test(self, duration_minutes: int = 30) -> Dict:
        """Run continuous processing stress test"""
        logger.info(f"🔥 Starting {duration_minutes}-minute stress test")
        
        stress_results = {
            'duration_minutes': duration_minutes,
            'iterations_completed': 0,
            'total_parcels_processed': 0,
            'errors_encountered': 0,
            'peak_memory_mb': 0
        }
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        while time.time() < end_time:
            try:
                # Run medium-scale test repeatedly
                iteration_result = self.run_single_scale_test(100)
                stress_results['iterations_completed'] += 1
                
                if iteration_result.get('success'):
                    stress_results['total_parcels_processed'] += iteration_result.get('actual_parcels', 0)
                else:
                    stress_results['errors_encountered'] += 1
                
                # Track memory usage
                memory_info = self._get_memory_usage()
                current_memory = memory_info.get('memory_mb', 0)
                if isinstance(current_memory, (int, float)) and current_memory > stress_results['peak_memory_mb']:
                    stress_results['peak_memory_mb'] = current_memory
                
                # Brief pause between iterations
                time.sleep(5)
                
            except KeyboardInterrupt:
                logger.info("Stress test interrupted by user")
                break
            except Exception as e:
                logger.error(f"Stress test iteration failed: {e}")
                stress_results['errors_encountered'] += 1
        
        actual_duration = (time.time() - start_time) / 60
        stress_results['actual_duration_minutes'] = round(actual_duration, 2)
        
        logger.info(f"🏁 Stress test complete: {stress_results['iterations_completed']} iterations, "
                   f"{stress_results['total_parcels_processed']} total parcels")
        
        return stress_results


# Global test suite instance
production_test_suite = ProductionTestSuite()