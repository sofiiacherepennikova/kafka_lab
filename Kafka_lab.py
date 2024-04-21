import time
import random
import json
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from csv import DictReader
import sys
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

from sklearn.ensemble import RandomForestRegressor


class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'], compression_type='lz4')

    def produce_message(self, topic_name, data):
        self.producer.send(topic_name, json.dumps(data).encode('utf-8'))

    def data_production(self, producer, input_file, topic_name):
        with open(input_file,'r') as read_obj:
            csv_dict_reader = DictReader(read_obj)
            for row in csv_dict_reader:
                producer.produce_message(topic_name, row)
                time.sleep(random.uniform(0.01, 0.02))


class Consumer:
    def __init__(self, input_topic, consumer_group, timeout):
        self.consumer = KafkaConsumer(input_topic,
                                      bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
                                      group_id=consumer_group,
                                      consumer_timeout_ms=timeout,
                                      value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                                      auto_offset_reset='earliest')       

    def process_data_to(self, result_topic_name):
        producer = Producer()
        for message in self.consumer:
            msg = message.value
            f = str(msg['track_genre'])
            if f == 'pop' or f == 'acoustic' or f == 'disco' or f == 'k-pop':
                producer.produce_message(result_topic_name, msg)

    def df_prepare(self):
        df = pd.DataFrame()
        keys_flag = False
        for message in self.consumer:
            msg = message.value
            if keys_flag == False:
                key_list=list(msg.keys())
                if key_list:
                    keys_flag = True
            df = pd.concat([df, pd.json_normalize(msg)])        
        df.columns = key_list
        df = df.drop_duplicates(subset=['track_name'])

        return df  

    def visual_figure_preparing(self, visualize_df):

        #Visualizing

        # Top of music ganres based on total popularity of tracks
        fig1 = px.bar(visualize_df.groupby('track_genre',as_index=False).sum().sort_values(by='popularity',ascending=False).head(50),x='track_genre',y='popularity',labels={'track_genre':'popularity'},width=1000,color_discrete_sequence=['green'],text='track_genre',title='<b> Music ganres popularity analysis')

        # Top-25 songs    
        fig2= px.bar(visualize_df.sort_values(by='popularity',ascending=False).head(50), x='track_name', y='popularity',labels={'artists':'artists'},color_discrete_sequence=['green'],text='artists',title='<b> Top 25 songs')

        # Get Top-3 genres list
        top3_genres = (visualize_df.groupby('track_genre',as_index=False).sum().sort_values(by='popularity',ascending=False).head(3))['track_genre'].unique()

        genre1_subframe = visualize_df[visualize_df['track_genre'] == top3_genres[0]]
        fig_genre1 = px.bar(genre1_subframe.sort_values(by='popularity', ascending=False).head(5), x='track_name', y='popularity', labels={'artists':'artists'}, color_discrete_sequence=['green'],text='artists',title=f"<b> Top 5 {top3_genres[0]} songs")

        genre2_subframe = visualize_df[visualize_df['track_genre'] == top3_genres[1]]
        fig_genre2 = px.bar(genre2_subframe.sort_values(by='popularity', ascending=False).head(5), x='track_name', y='popularity', labels={'artists':'artists'}, color_discrete_sequence=['green'],text='artists',title=f"<b> Top 5 {top3_genres[1]} songs")
                
        genre3_subframe = visualize_df[visualize_df['track_genre'] == top3_genres[2]]
        fig_genre3 = px.bar(genre3_subframe.sort_values(by='popularity', ascending=False).head(5), x='track_name', y='popularity', labels={'artists':'artists'}, color_discrete_sequence=['green'],text='artists',title=f"<b> Top 5 {top3_genres[2]} songs")


        fig=make_subplots(rows=6,cols=3,
                          subplot_titles=('<i>Popular genres', '<i>Top25 Songs', f'<i> Top 5 {top3_genres[0]} songs', f'<i> Top 5 {top3_genres[1]} songs', f'<i> Top 5 {top3_genres[2]} songs', '<i>popularity', '<i>danceability', '<i>energy', '<i>loudness', '<i>speechiness', '<i>acousticness', '<i>liveness', '<i>valence', '<i>tempo'), 
                          specs=[[{"colspan": 3}, None, None], 
                                 [{"colspan": 3}, None, None], 
                                 [{}, {}, {}],
                                 [{}, {}, {}],
                                 [{}, {}, {}],
                                 [{}, {}, {}]])
        fig.add_trace(fig1['data'][0],row=1,col=1)
        fig.add_trace(fig2['data'][0],row=2,col=1)
        fig.add_trace(fig_genre1['data'][0],row=3,col=1)
        fig.add_trace(fig_genre2['data'][0],row=3,col=2)
        fig.add_trace(fig_genre3['data'][0],row=3,col=3)
        fig.add_trace(go.Histogram(x=visualize_df['popularity'],name='popularity'),row=4,col=1)
        fig.add_trace(go.Histogram(x=visualize_df['danceability'],name='danceability'),row=4,col=2)
        fig.add_trace(go.Histogram(x=visualize_df['energy'],name='energy'),row=4,col=3)
        fig.add_trace(go.Histogram(x=visualize_df['loudness'],name='loudness'),row=5,col=1)
        fig.add_trace(go.Histogram(x=visualize_df['speechiness'],name='speechiness'),row=5,col=2)
        fig.add_trace(go.Histogram(x=visualize_df['acousticness'],name='acousticness'),row=5,col=3)
        fig.add_trace(go.Histogram(x=visualize_df['liveness'],name='liveness'),row=6,col=1)
        fig.add_trace(go.Histogram(x=visualize_df['valence'],name='valence'),row=6,col=2)
        fig.add_trace(go.Histogram(x=visualize_df['tempo'],name='tempo'),row=6,col=3)
        fig.update_layout(height=1080,width=1920,title_text='<b>Spotify Track Visualization')
        fig.update_layout(template='plotly',title_x=0.5)
        return fig

    def visualize_data(self, general_dataframe, result_topic_name):
        df = general_dataframe.copy()

        visualize_df = pd.DataFrame()
        visualize_df = df[['artists', 'album_name', 'track_name', 'popularity', 'track_genre', 'danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'liveness', 'valence', 'tempo']].copy()
        visualize_df['popularity'] = visualize_df['popularity'].astype(str).astype(int)     

        # PRODUCE THE visualize_df Dataframe into 'data_for_visualization' TOPIC
        track_list = visualize_df.to_dict(orient='records')
        producer = Producer()
        for track in range(0, len(track_list)):
            producer.produce_message(result_topic_name, track_list[track])

        # SHOW THE VISUALIZATION
        fig = self.visual_figure_preparing(visualize_df)
        fig.show()

    def popularity_prediction(self, general_dataframe, result_topic_name):
        working_df = general_dataframe.copy()

        working_df = working_df.dropna()
        working_df = working_df[['artists', 'popularity', 'track_genre', 'danceability', 'loudness']].copy()
        working_df = pd.get_dummies(working_df, columns=['artists', 'track_genre'])
        # Select ratio
        ratio = 0.05
        
        total_rows = working_df.shape[0]
        train_size = int(total_rows*ratio)
        
        # Split data into test and train
        train = working_df[0:train_size]
        validate = working_df[train_size:(2*train_size)]
        test = working_df[(2*train_size):]
        X_train = train.drop(columns=['popularity'])
        y_train = train['popularity']
        # X_test = test.drop(columns=['popularity'])
        # y_test = test['popularity']
        X_validate = validate.drop(columns=['popularity'])
        y_validate = validate['popularity']
    
        model = RandomForestRegressor()
        model.fit(X_train, y_train)

        y_pred1 = model.predict(X_validate)
        
        prediction_df = pd.DataFrame(y_pred1)
        # SOME DATAFRAME PROCESSING + ML LOGIC. As the result we suppose to have 'prediction_df' data frame: [track_id predicted_popularity original_popularity prediction_error]. Of course, you can choose any output you want!!

        # PRODUCE THE prediction output dataframe into 'prediction_data' TOPIC
        track_list = prediction_df.to_dict(orient='records')
        producer = Producer()
        for track in range(0, len(track_list)):
            producer.produce_message(result_topic_name, track_list[track])

def main():
        # PRODUCE THE DATASET FROM CSV TO 'raw_data' KAFKA TOPIC
        producer = Producer()
        producer.data_production(producer, 'Spotify_Tracks_Dataset.csv', 'raw_data')

        # 1) CONSUME THE RAW DATA FROM 'raw_data' TOPIC
        # 2) DO SOME SIMPLE FILTRATION - CHOOSE THE POP, ACOUSTIC, DISCO and K-POP TRACKS
        # 3) PRODUCE THE GOTTEN POP TRACK TO 'processed_data' KAFKA TOPIC
        process_consumer = Consumer('raw_data', 'process_consumer', 60000)
        process_consumer.process_data_to('processed_data')


        # 1) CONSUME THE RAW DATA FROM 'raw_data' TOPIC
        # 2) DO SOME PANDAS DATAFRAME PREPARATIONS
        # 3) PRODUCE THE VISUALIZED_DF TO 'visual_data' KAFKA TOPIC
        visualize_consumer = Consumer('raw_data', 'visual_consumer', 2400000)
        general_dataframe = visualize_consumer.df_prepare()
        visualize_consumer.visualize_data(general_dataframe, 'visual_data')

        # 1) CONSUME THE RAW DATA FROM 'raw_data' TOPIC
        # 2) DO SOME PANDAS DATAFRAME PREPARATIONS
        # 3) DO SOME MACHINE LEARNING TO PREDICT TRACK POPULARITY 
        # 3) PRODUCE THE PREDICTED DATA TO 'ml_results' KAFKA TOPIC
        ml_consumer = Consumer('raw_data', 'ml_consumer', 2400000)
        general_dataframe = visualize_consumer.df_prepare()        
        ml_consumer.popularity_prediction(general_dataframe, 'ml_results')



if __name__ == "__main__":
    main()
